/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moz.fiji.rest.resources;

import static com.moz.fiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static com.moz.fiji.rest.RoutesConstants.ROWS_PATH;
import static com.moz.fiji.rest.RoutesConstants.TABLE_PARAMETER;
import static com.moz.fiji.rest.util.RowResourceUtil.addColumnDefs;
import static com.moz.fiji.rest.util.RowResourceUtil.getFijiRestRow;
import static com.moz.fiji.rest.util.RowResourceUtil.getTimestamps;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HConstants;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.rest.FijiClient;
import com.moz.fiji.rest.representations.FijiRestEntityId;
import com.moz.fiji.rest.representations.FijiRestRow;
import com.moz.fiji.rest.util.RowResourceUtil;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiBufferedWriter;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.filter.FormattedEntityIdRowFilter;
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * This REST resource interacts with Fiji tables.
 *
 * This resource is served for requests using the resource identifier: <li>
 * /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/rows
 */
@Path(ROWS_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class RowsResource {
  private static final String UNLIMITED_VERSIONS = "all";

  private final FijiClient mFijiClient;

  /**
   * Special constant to denote stream unlimited amount of rows
   * to the client.
   */
  private static final int UNLIMITED_ROWS = -1;

  /**
   * Since we are streaming the rows to the user, we need access to the object mapper
   * used by DropWizard to convert objects to JSON.
   */
  private final ObjectMapper mJsonObjectMapper;

  /**
   * Special constant to denote that all columns are to be selected.
   */
  public static final String ALL_COLS = "*";

  /**
   * Default constructor.
   *
   * @param fijiClient that this should use for connecting to Fiji.
   * @param jsonObjectMapper is the ObjectMapper used by DropWizard to convert from Java
   *        objects to JSON.
   */
  public RowsResource(final FijiClient fijiClient, final ObjectMapper jsonObjectMapper) {
    mFijiClient = fijiClient;
    mJsonObjectMapper = jsonObjectMapper;
  }

  /**
   * Class to support streaming FijiRows to the client.
   *
   */
  private class RowStreamer implements StreamingOutput {

    private Iterable<FijiRowData> mScanner = null;
    private final FijiTable mTable;
    private final FijiSchemaTable mSchemaTable;

    private int mNumRows = 0;
    private final List<FijiColumnName> mColsRequested;

    /**
     * Construct a new RowStreamer.
     *
     * @param scanner is the iterator over FijiRowData.
     * @param table the table from which the rows originate.
     * @param numRows is the maximum number of rows to stream.
     * @param columns are the columns requested by the client.
     * @param schemaTable is the handle to the FijiSchemaTable used to encode the cell's writer
     *        schema as a UID.
     */
    public RowStreamer(Iterable<FijiRowData> scanner, FijiTable table, int numRows,
        List<FijiColumnName> columns, FijiSchemaTable schemaTable) {
      mScanner = scanner;
      mTable = table;
      mNumRows = numRows;
      mColsRequested = columns;
      mSchemaTable = schemaTable;
    }

    /**
     * Performs the actual streaming of the rows.
     *
     * @param os is the OutputStream where the results are written.
     */
    @Override
    public void write(OutputStream os) {
      int numRows = 0;
      Writer writer = new BufferedWriter(new OutputStreamWriter(os, Charset.forName("UTF-8")));
      Iterator<FijiRowData> it = mScanner.iterator();
      boolean clientClosed = false;

      try {
        while (it.hasNext() && (numRows < mNumRows || mNumRows == UNLIMITED_ROWS)
            && !clientClosed) {
          FijiRowData row = it.next();
          FijiRestRow restRow = getFijiRestRow(row, mTable.getLayout(), mColsRequested,
              mSchemaTable);
          String jsonResult = mJsonObjectMapper.writeValueAsString(restRow);
          // Let's strip out any carriage return + line feeds and replace them with just
          // line feeds. Therefore we can safely delimit individual json messages on the
          // carriage return + line feed for clients to parse properly.
          jsonResult = jsonResult.replaceAll("\r\n", "\n");
          writer.write(jsonResult + "\r\n");
          writer.flush();
          numRows++;
        }
      } catch (IOException e) {
        clientClosed = true;
      } finally {
        if (mScanner instanceof FijiRowScanner) {
          try {
            ((FijiRowScanner) mScanner).close();
          } catch (IOException e1) {
            throw new WebApplicationException(e1, Status.INTERNAL_SERVER_ERROR);
          }
        }
      }

      if (!clientClosed) {
        try {
          writer.flush();
          writer.close();
        } catch (IOException e) {
          throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
        }
      }
    }
  }

  /**
   * Resolves an iterable collection of FijiRestEntityIds to EntityId object.
   * This does not handle wildcards
   *
   * @param fijiRestEntityIds list of entity ids to be resolved.
   * @param layout FijiTableLayout to resolve the ids.
   * @return a list of entity ids.
   * @throws IOException if resolving an id fails
   */
  private List<EntityId> getEntityIdsFromFijiRestEntityIds(
      List<FijiRestEntityId> fijiRestEntityIds,
      FijiTableLayout layout)
      throws IOException {
    Set<EntityId> entityIds = Sets.newHashSet();

    for (FijiRestEntityId fijiRestEntityId : fijiRestEntityIds) {
      EntityId eid = fijiRestEntityId.resolve(layout);
      if (!entityIds.contains(eid)) {
        entityIds.add(eid);
      }
    }

    return Lists.newArrayList(entityIds);
  }

  /**
   * Returns the number of true parameters inputted.
   *
   * @param cases array of cases to be tested.
   * @return number of true cases.
   */
  private int countTrue(boolean... cases) {
    int result = 0;
    for (boolean c : cases) {
      if (c) {
        result++;
      }
    }
    return result;
  }

  /**
   * GETs a list of Fiji rows.
   *
   * @param instance is the instance where the table resides.
   * @param table is the table where the rows from which the rows will be streamed
   * @param jsonEntityId the entity_id of the row to return.
   * @param jsonEntityIds a JSON array of the entity_ids of the rows to bulk return. Wildcards are
   *        not supported when using this parameter.
   * @param startEidString the left endpoint eid of the range scan.
   * @param endEidString the right endpoint eid of the range scan.
   * @param limit the maximum number of rows to return. Set to -1 to stream all rows.
   * @param columns is a comma separated list of columns (either family or family:qualifier) to
   *        fetch
   * @param maxVersionsString is the max versions per column to return.
   *        Can be "all" for all versions.
   * @param timeRange is the time range of cells to return (specified by min..max where min/max is
   *        the ms since UNIX epoch. min and max are both optional; however, if something is
   *        specified, at least one of min/max must be present.)
   * @param uriInfo contains all the query parameters.
   * @return the Response object containing the rows requested in JSON
   */
  @GET
  @Timed
  @ApiStability.Experimental
  // CSOFF: ParameterNumberCheck - There are a bunch of query param options
  public Response getRows(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @QueryParam("eid") String jsonEntityId,
      @QueryParam("eids") String jsonEntityIds,
      @QueryParam("start_eid") String startEidString,
      @QueryParam("end_eid") String endEidString,
      @QueryParam("limit") @DefaultValue("100") int limit,
      @QueryParam("cols") @DefaultValue(ALL_COLS) String columns,
      @QueryParam("versions") @DefaultValue("1") String maxVersionsString,
      @QueryParam("timerange") String timeRange,
      @Context UriInfo uriInfo) {
    // CSON: ParameterNumberCheck - There are a bunch of query param options
    long[] timeRanges = null;
    FijiTable fijiTable = mFijiClient.getFijiTable(instance, table);
    FijiTableLayout layout = fijiTable.getLayout();
    Iterable<FijiRowData> scanner = null;
    int maxVersions;
    FijiDataRequestBuilder dataBuilder = FijiDataRequest.builder();
    if (timeRange != null) {
      timeRanges = getTimestamps(timeRange);
    }
    try {
      if (UNLIMITED_VERSIONS.equalsIgnoreCase(maxVersionsString)) {
        maxVersions = HConstants.ALL_VERSIONS;
      } else {
        maxVersions = Integer.parseInt(maxVersionsString);
      }
    } catch (NumberFormatException nfe) {
      throw new WebApplicationException(nfe, Status.BAD_REQUEST);
    }
    if (timeRange != null) {
      dataBuilder.withTimeRange(timeRanges[0], timeRanges[1]);
    }
    ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(maxVersions);
    List<FijiColumnName> requestedColumns = addColumnDefs(layout, colsRequested,
        columns);

    /* Check that the row retrieval method is valid, only one of the following may be true:
     *  @eid has a value for single gets,
     *  @eids has a value for bulk gets,
     *  @start_eid or @end_eid has a value for scanned gets.
     */
    if (countTrue(jsonEntityId != null, (startEidString != null || endEidString != null),
        jsonEntityIds != null) > 1) {
      throw new WebApplicationException(new IllegalArgumentException("Ambiguous request. "
          + "Specified more than one entity Id search method."), Status.BAD_REQUEST);
    }

    FijiTableReader reader = null;
    try {
      if (jsonEntityId != null) {
        final FijiRestEntityId fijiRestEntityId =
            FijiRestEntityId.createFromUrl(jsonEntityId, layout);
        if (fijiRestEntityId.isWildcarded()) {
          // Wildcards were found, continue with FormattedEntityIdRowFilter.
          final FijiRowFilter entityIdRowFilter =
              new FormattedEntityIdRowFilter(
                  (RowKeyFormat2) layout.getDesc().getKeysFormat(),
                  fijiRestEntityId.getComponents());
          reader = fijiTable.openTableReader();
          final FijiScannerOptions scanOptions = new FijiScannerOptions();
          scanOptions.setFijiRowFilter(entityIdRowFilter);
          scanner = reader.getScanner(dataBuilder.build(), scanOptions);
        } else {
          // No wildcards found, but potentially valid entity id.
          // Continue scanning point row.
          final EntityId eid = fijiRestEntityId.resolve(layout);
          final FijiDataRequest request = dataBuilder.build();
          scanner = ImmutableList.of(RowResourceUtil.getFijiRowData(fijiTable, eid, request));
        }
      } else if (jsonEntityIds != null) {
        // If there are wildcards in the json array, creating and entity id list will
        // throw and exception.
        final List<FijiRestEntityId> fijiRestEntityIds =
            FijiRestEntityId.createListFromUrl(jsonEntityIds, layout);
        reader = fijiTable.openTableReader();
        scanner = reader.bulkGet(
            getEntityIdsFromFijiRestEntityIds(fijiRestEntityIds, layout),
            dataBuilder.build());
      } else {
        // Single eid not provided. Continue with a range scan.
        final FijiScannerOptions scanOptions = new FijiScannerOptions();
        if (startEidString != null) {
          final EntityId eid =
              FijiRestEntityId.createFromUrl(startEidString, null).resolve(layout);
          scanOptions.setStartRow(eid);
        }
        if (endEidString != null) {
          final EntityId eid =
              FijiRestEntityId.createFromUrl(endEidString, null).resolve(layout);
          scanOptions.setStopRow(eid);
        }
        reader = fijiTable.openTableReader();
        scanner = reader.getScanner(dataBuilder.build(), scanOptions);
      }
    } catch (FijiIOException kioe) {
      mFijiClient.invalidateTable(instance, table);
      throw new WebApplicationException(kioe, Status.BAD_REQUEST);
    } catch (JsonProcessingException jpe) {
      throw new WebApplicationException(jpe, Status.BAD_REQUEST);
    } catch (Exception e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    } finally {
      // If reader was used, close it.
      if (null != reader) {
        ResourceUtils.closeOrLog(reader);
      }
    }
    FijiSchemaTable schemaTable = mFijiClient.getFijiSchemaTable(instance);
    return Response.ok(new RowStreamer(scanner, fijiTable, limit, requestedColumns,
        schemaTable)).build();
  }


  /**
   * Commits a FijiRestRow representation to the fiji table: performs create and update.
   * Note that the user-formatted entityId is required.
   * Also note that writer schema is not considered as of the latest version.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param fijiRestRow POST-ed json data
   * @return a message containing the rowkey of interest
   * @throws IOException when post fails
   */
  private Map<String, String> postRow(final String instance,
      final String table,
      final FijiRestRow fijiRestRow)
      throws IOException {
    final FijiTable fijiTable = mFijiClient.getFijiTable(instance, table);

    final EntityId entityId;
    if (null != fijiRestRow.getEntityId()) {
      entityId = fijiRestRow.getEntityId().resolve(fijiTable.getLayout());
    } else {
      throw new WebApplicationException(
          new IllegalArgumentException("EntityId was not specified."), Status.BAD_REQUEST);
    }

    // Open writer and write.
    RowResourceUtil.writeRow(fijiTable, entityId, fijiRestRow,
        mFijiClient.getFijiSchemaTable(instance));

    // Better output?
    Map<String, String> returnedTarget = Maps.newHashMap();

    URI targetResource = UriBuilder.fromResource(RowsResource.class).build(instance, table);
    String eidString = URLEncoder.encode(fijiRestRow.getEntityId().toString(), "UTF-8");

    returnedTarget.put("target", targetResource.toString() + "?eid=" + eidString);

    return returnedTarget;

  }

  /**
   * POSTs JSON body to row(s): performs create and update.
   * The input JSON blob can either represent a single FijiRestRow or a list of FijiRestRows.
   *
   * For example, a single FijiRestRow:
   * {
   *   "entityId":"hbase=hex:8c2d2fcc2c150efb49ce0817e1823d46",
   *   "cells":{
   *       "info":{
   *          "firstname":[
   *             {
   *                "timestamp":123,
   *                "value":"John"
   *             }
   *          ]
   *       },
   *       "info":{
   *          "lastname":[
   *             {
   *                "timestamp":123,
   *                "value":"Smith"
   *             }
   *          ]
   *       }
   *    }
   * }
   *
   * A list of FijiRestRows:
   * [
   *    {
   *       "entityId":"hbase=hex:8c2d2fcc2c150efb49ce0817e1823d46",
   *       "cells":{
   *          "info":{
   *             "firstname":[
   *                {
   *                   "timestamp":123,
   *                   "value":"John"
   *                }
   *             ]
   *          }
   *       }
   *    },
   *    {
   *       "entityId":"hbase=hex:acfbe1234567890987654321abcfdega",
   *       "cells":{
   *          "info":{
   *             "firstname":[
   *                {
   *                   "timestamp":12312345,
   *                   "value":"Jane"
   *                }
   *             ]
   *          }
   *       }
   *    }
   * ]
   *
   * Note that the user-formatted entityId is required.
   * Also note that writer schema is not considered as of the latest version.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param fijiRestRows POST-ed json data
   * @return a message containing the rowkey of interest
   * @throws IOException when post fails
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiStability.Experimental
  public Map<String, List<String>> postRows(@PathParam(INSTANCE_PARAMETER) final String instance,
      @PathParam(TABLE_PARAMETER) final String table,
      final JsonNode fijiRestRows)
      throws IOException {
    // We intend to return a JSON blob listing the row keys we are putting to.
    // i.e. {targets : [..., ..., ...]}
    final List<String> results = Lists.newLinkedList();

    final Iterator<JsonNode> rowIterator;
    if (fijiRestRows.isArray()) {
      rowIterator = fijiRestRows.elements();
    } else {
      rowIterator = Iterators.singletonIterator(fijiRestRows);
    }

    // Put each row.
    while (rowIterator.hasNext()) {
      final FijiRestRow fijiRestRow = mJsonObjectMapper
          .treeToValue(rowIterator.next(), FijiRestRow.class);
      final Map<String, String> result = postRow(instance, table, fijiRestRow);
      results.add(result.get("target"));
    }

    final Map<String, List<String>> returnedResults = Maps.newHashMap();
    returnedResults.put("targets", results);
    return returnedResults;
  }

  /**
   * DELETEs a Fiji row, a list of columns in a row, a list of rows, or a list of columns in a list
   * of rows using a buffered write. This method does not support wildcards.
   *
   * @param instance is the instance where the table resides.
   * @param table is the table where the rows from which the rows will be deleted.
   * @param jsonEntityId the entity id or row key of the row to delete.
   * @param jsonEntityIds a JSON array of entity ids or row keys rows to delete. Will cause an error
   *        if the jsonEntityId is also specified.
   * @param columns is a comma separated list of columns (either family or family:qualifier) to
   *        delete.
   * @param timestamp is the time stamp that denotes which cells to delete. All cells with
   *        time stamps before the supplied time stamp will be deleted.
   * @return whether the method completed successfully (true unless exception occurred)
   */
  @DELETE
  @Timed
  @ApiStability.Experimental
  public boolean deleteRows(
      @PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @QueryParam("eid") String jsonEntityId,
      @QueryParam("eids") String jsonEntityIds,
      @QueryParam("cols") @DefaultValue(ALL_COLS) String columns,
      @QueryParam("timestamp") @DefaultValue("-1") Long timestamp) {
    FijiTable fijiTable = mFijiClient.getFijiTable(instance, table);
    FijiTableLayout layout = fijiTable.getLayout();

    if (jsonEntityId != null && jsonEntityIds != null) {
      throw new WebApplicationException(new IllegalArgumentException("Ambiguous request. "
          + "Specified both jsonEntityId and jsonEntityIds."), Status.BAD_REQUEST);
    } else if (jsonEntityId == null && jsonEntityIds == null) {
      throw new WebApplicationException(new IllegalArgumentException("Ambiguous request. "
          + "Specified neither jsonEntityId or jsonEntityIds."), Status.BAD_REQUEST);
    }

    try {
      List<FijiRestEntityId> fijiRestEntityIds = Lists.newArrayList();

      if (jsonEntityId != null) {
        fijiRestEntityIds.add(FijiRestEntityId.createFromUrl(jsonEntityId, layout));
      } else {
        fijiRestEntityIds.addAll(FijiRestEntityId.createListFromUrl(jsonEntityIds, layout));
      }

      List<EntityId> entityIds = getEntityIdsFromFijiRestEntityIds(fijiRestEntityIds, layout);

      final FijiBufferedWriter writer = fijiTable.getWriterFactory().openBufferedWriter();

      for (EntityId eid : entityIds) {
        if (columns.equals(ALL_COLS)) {
            if (timestamp >= 0) {
              writer.deleteRow(eid, timestamp);
            } else {
              writer.deleteRow(eid);
            }
        } else {
          String[] requestedColumnArray = columns.split(",");
          for (String s : requestedColumnArray) {
            FijiColumnName columnName = new FijiColumnName(s);
            if (timestamp >= 0) {
              writer.deleteColumn(
                  eid, columnName.getFamily(), columnName.getQualifier(), timestamp);
            } else {
              writer.deleteColumn(eid, columnName.getFamily(), columnName.getQualifier());
            }
          }
        }
      }

      writer.flush();
      writer.close();
    } catch (IOException ioe) {
      throw new WebApplicationException(ioe, Status.BAD_REQUEST);
    }
    return true;
  }
}
