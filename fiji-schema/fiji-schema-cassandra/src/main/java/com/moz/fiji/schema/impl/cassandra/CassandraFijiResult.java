/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.cassandra.CassandraColumnName;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.impl.DefaultFijiResult;
import com.moz.fiji.schema.impl.EmptyFijiResult;
import com.moz.fiji.schema.impl.MaterializedFijiResult;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;
import com.moz.fiji.schema.layout.impl.ColumnId;

/**
 * A utility class which can create a {@link FijiResult} view on a Cassandra Fiji table.
 */
@ApiAudience.Private
public final class CassandraFijiResult {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiResult.class);

  /**
   * Create a new {@link FijiResult} backed by Cassandra.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest FijiDataRequest defining the values to retrieve.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The type of value in the {@code FijiCell} of this view.
   * @return an {@code FijiResult}.
   * @throws IOException On error while decoding cells.
   */
  public static <T> FijiResult<T> create(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final CassandraFijiTable table,
      final FijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    final FijiDataRequestBuilder unpagedRequestBuilder = FijiDataRequest.builder();
    final FijiDataRequestBuilder pagedRequestBuilder = FijiDataRequest.builder();
    unpagedRequestBuilder.withTimeRange(
        dataRequest.getMinTimestamp(),
        dataRequest.getMaxTimestamp());
    pagedRequestBuilder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());

    for (final Column columnRequest : dataRequest.getColumns()) {
      if (columnRequest.getFilter() != null) {
        throw new UnsupportedOperationException(
            String.format("Cassandra Fiji does not support filters on column requests: %s.",
                columnRequest));
      }
      if (columnRequest.isPagingEnabled()) {
        pagedRequestBuilder.newColumnsDef(columnRequest);
      } else {
        unpagedRequestBuilder.newColumnsDef(columnRequest);
      }
    }

    final CellDecoderProvider requestDecoderProvider =
        decoderProvider.getDecoderProviderForRequest(dataRequest);

    final FijiDataRequest unpagedRequest = unpagedRequestBuilder.build();
    final FijiDataRequest pagedRequest = pagedRequestBuilder.build();

    if (unpagedRequest.isEmpty() && pagedRequest.isEmpty()) {
      return new EmptyFijiResult<>(entityId, dataRequest);
    }

    final MaterializedFijiResult<T> materializedFijiResult;
    if (!unpagedRequest.isEmpty()) {
      materializedFijiResult =
          createMaterialized(
              table,
              entityId,
              unpagedRequest,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      materializedFijiResult = null;
    }

    final CassandraPagedFijiResult<T> pagedFijiResult;
    if (!pagedRequest.isEmpty()) {
      pagedFijiResult =
          new CassandraPagedFijiResult<>(
              entityId,
              pagedRequest,
              table,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      pagedFijiResult = null;
    }

    if (unpagedRequest.isEmpty()) {
      return pagedFijiResult;
    } else if (pagedRequest.isEmpty()) {
      return materializedFijiResult;
    } else {
      return DefaultFijiResult.create(dataRequest, materializedFijiResult, pagedFijiResult);
    }
  }

  /**
   * Create a materialized {@code FijiResult} for a get on a Cassandra Fiji table.
   *
   * @param table The Cassandra Fiji table.
   * @param entityId The entity ID of the row to get.
   * @param dataRequest The data request defining the columns to get. All columns must be non-paged.
   * @param layout The layout of the table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   * @param <T> The value type of cells in the result.
   * @return A materialized {@code FijiResult} for the row.
   */
  public static <T> MaterializedFijiResult<T> createMaterialized(
      final CassandraFijiTable table,
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final FijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {

    SortedMap<FijiColumnName, ListenableFuture<Iterator<FijiCell<T>>>> resultFutures =
        Maps.newTreeMap();

    for (final Column columnRequest : dataRequest.getColumns()) {
      Preconditions.checkArgument(
          !columnRequest.isPagingEnabled(),
          "CassandraMaterializedFijiResult can not be created with a paged data request: %s.",
          dataRequest);

      resultFutures.put(
          columnRequest.getColumnName(),
          CassandraFijiResult.<T>getColumn(
              table,
              entityId,
              columnRequest,
              dataRequest,
              layout,
              translator,
              decoderProvider));
    }

    SortedMap<FijiColumnName, List<FijiCell<T>>> results = Maps.newTreeMap();
    for (Map.Entry<FijiColumnName, ListenableFuture<Iterator<FijiCell<T>>>> entry
        : resultFutures.entrySet()) {

      results.put(
          entry.getKey(),
          Lists.newArrayList(CassandraFijiResult.unwrapFuture(entry.getValue())));
    }

    return MaterializedFijiResult.create(entityId, dataRequest, layout, results);
  }

  /**
   * Query Cassandra for a Fiji qualified-column or column-family in a Fiji row. The result is a
   * future containing an iterator over the result cells.
   *
   * @param table The Cassandra Fiji table.
   * @param entityId The entity ID of the row in the Fiji table.
   * @param columnRequest The requested column.
   * @param dataRequest The data request defining the request options.
   * @param layout The table's layout.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   * @param <T> The value type of the column.
   * @return A future containing an iterator of cells in the column.
   */
  public static <T> ListenableFuture<Iterator<FijiCell<T>>> getColumn(
      final CassandraFijiTable table,
      final EntityId entityId,
      final Column columnRequest,
      final FijiDataRequest dataRequest,
      final FijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    final FijiURI tableURI = table.getURI();
    final FijiColumnName column = columnRequest.getColumnName();
    final CassandraColumnName cassandraColumn;
    try {
      cassandraColumn = translator.toCassandraColumnName(column);
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(
          String.format("No such column '%s' in table %s.", column, tableURI));
    }

    final ColumnId localityGroupId =
        layout.getFamilyMap().get(column.getFamily()).getLocalityGroup().getId();
    final CassandraTableName tableName =
        CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupId);

    final CQLStatementCache statementCache = table.getStatementCache();

    if (column.isFullyQualified()) {
      final Statement statement =
          statementCache.createGetStatement(
              tableName,
              entityId,
              cassandraColumn,
              dataRequest,
              columnRequest);

      return Futures.transform(
          table.getAdmin().executeAsync(statement),
          RowDecoders.<T>getQualifiedColumnDecoderFunction(column, decoderProvider));
    } else {

      if (columnRequest.getMaxVersions() != 0) {
        LOG.warn("Cassandra Fiji can not efficiently get a column family with max versions"
                + " (column family: {}, max version: {}). Filtering versions on the client.",
            column, columnRequest.getMaxVersions());
      }

      if (dataRequest.getMaxTimestamp() != Long.MAX_VALUE
          || dataRequest.getMinTimestamp() != Long.MIN_VALUE) {
        LOG.warn("Cassandra Fiji can not efficiently restrict a timestamp on a column family: "
                + " (column family: {}, data request: {}). Filtering timestamps on the client.",
            column, dataRequest);
      }

      final Statement statement =
          statementCache.createGetStatement(
              tableName,
              entityId,
              cassandraColumn,
              dataRequest,
              columnRequest);

      return Futures.transform(
          table.getAdmin().executeAsync(statement),
          RowDecoders.<T>getColumnFamilyDecoderFunction(
              tableName,
              column,
              columnRequest,
              dataRequest,
              layout,
              translator,
              decoderProvider));
    }
  }

  /**
   * Unwrap a Cassandra listenable future.
   *
   * @param future The future to unwrap.
   * @param <T> The value type of the future.
   * @return The future's value.
   */
  public static <T> T unwrapFuture(final ListenableFuture<T> future) {
    // See DefaultResultSetFuture#getUninterruptibly
    try {
      return Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof DriverException) {
        throw (DriverException) cause;
      } else {
        throw new DriverInternalError("Unexpected exception thrown", cause);
      }
    }
  }

  /**
   * Constructor for non-instantiable helper class.
   */
  private CassandraFijiResult() { }
}
