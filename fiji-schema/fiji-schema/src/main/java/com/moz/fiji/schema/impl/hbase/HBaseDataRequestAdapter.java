/**
 * (c) Copyright 2012 WibiData, Inc.
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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.filter.FijiColumnFilter;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.hbase.HBaseScanOptions;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.platform.SchemaPlatformBridge;

/**
 * Wraps a FijiDataRequest to expose methods that generate meaningful objects in HBase
 * land, like {@link org.apache.hadoop.hbase.client.Put}s and {@link
 * org.apache.hadoop.hbase.client.Get}s.
 */
@ApiAudience.Private
public final class HBaseDataRequestAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseDataRequestAdapter.class);

  /** The wrapped FijiDataRequest. */
  private final FijiDataRequest mFijiDataRequest;
  /** The translator for generating HBase column names. */
  private final HBaseColumnNameTranslator mColumnNameTranslator;

  /**
   * Creates a new HBaseDataRequestAdapter for a given data request using a given
   * FijiColumnNameTranslator.
   *
   * @param fijiDataRequest the data request to adapt for HBase.
   * @param translator the name translator for getting HBase column names.
   */
  public HBaseDataRequestAdapter(
      final FijiDataRequest fijiDataRequest,
      final HBaseColumnNameTranslator translator
  ) {
    mFijiDataRequest = fijiDataRequest;
    mColumnNameTranslator = translator;
  }

  /**
   * Constructs an HBase Scan that describes the data requested in the FijiDataRequest.
   *
   * @param tableLayout The layout of the Fiji table to read from.  This is required for
   *     determining the mapping between Fiji columns and HBase columns.
   * @return An HBase Scan descriptor.
   * @throws IOException If there is an error.
   */
  public Scan toScan(final FijiTableLayout tableLayout) throws IOException {
    return toScan(tableLayout, new HBaseScanOptions());
  }

  /**
   * Constructs an HBase Scan that describes the data requested in the FijiDataRequest.
   *
   * @param tableLayout The layout of the Fiji table to read from.  This is required for
   *     determining the mapping between Fiji columns and HBase columns.
   * @param scanOptions Custom options for this scan.
   * @return An HBase Scan descriptor.
   * @throws IOException If there is an error.
   */
  public Scan toScan(
      final FijiTableLayout tableLayout,
      final HBaseScanOptions scanOptions
  ) throws IOException {
    // Unfortunately in HBase 95+, we can no longer create empty gets.
    // So create a fake one for this table and fill in the fields of a new scan.
    final Get tempGet = toGet(HBaseEntityId.fromHBaseRowKey(new byte[1]), tableLayout);
    final Scan scan = new Scan();
    scan.setFilter(tempGet.getFilter());
    scan.setCacheBlocks(tempGet.getCacheBlocks());
    scan.setMaxVersions(tempGet.getMaxVersions());
    scan.setTimeRange(tempGet.getTimeRange().getMin(), tempGet.getTimeRange().getMax());
    scan.setFamilyMap(tempGet.getFamilyMap());
    configureScan(scan, scanOptions);
    return scan;
  }

  /**
   * Like toScan(), but mutates a given Scan object to include everything in the data
   * request instead of returning a new one.
   *
   * <p>Any existing request settings in the Scan object will be preserved.</p>
   *
   * @param scan The existing scan object to apply the data request to.
   * @param tableLayout The layout of the Fiji table the scan will read from.
   * @throws IOException If there is an error.
   */
  public void applyToScan(
      final Scan scan,
      final FijiTableLayout tableLayout
  ) throws IOException {
    final Scan newScan = toScan(tableLayout);

    // It's okay to put columns into the Scan that are already there.
    for (Map.Entry<byte[], NavigableSet<byte[]>> columnRequest
             : newScan.getFamilyMap().entrySet()) {
      byte[] family = columnRequest.getKey();
      if (null == columnRequest.getValue()) {
        // Request all columns in the family.
        scan.addFamily(family);
      } else {
        // Calls to Scan.addColumn() will invalidate any previous calls to Scan.addFamily(),
        // so we only do it if:
        //   1. No data from the family has been added to the request yet, OR
        //   2. Only specific columns from the family have been requested so far.
        if (!scan.getFamilyMap().containsKey(family)
            || null != scan.getFamilyMap().get(family)) {
          for (byte[] qualifier : columnRequest.getValue()) {
            scan.addColumn(family, qualifier);
          }
        }
      }
    }
  }

  /**
   * Constructs an HBase Get that describes the data requested in the FijiDataRequest for
   * a particular entity/row.
   *
   * @param entityId The row to build an HBase Get request for.
   * @param tableLayout The layout of the Fiji table to read from.  This is required for
   *     determining the mapping between Fiji columns and HBase columns.
   * @return An HBase Get descriptor.
   * @throws IOException If there is an error.
   */
  public Get toGet(
      final EntityId entityId,
      final FijiTableLayout tableLayout
  ) throws IOException {

    // Context to translate user Fiji filters into HBase filters:
    final FijiColumnFilter.Context filterContext =
        new NameTranslatingFilterContext(mColumnNameTranslator);

    // Get request we are building and returning:
    final Get get = new Get(entityId.getHBaseRowKey());

    // Filters for each requested column: OR(<filter-for-column-1>, <filter-for-column2>, ...)
    final FilterList columnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    // There's a shortcoming in the HBase API that doesn't allow us to specify per-column
    // filters for timestamp ranges and max versions.  We need to generate a request that
    // will include all versions that we need, and add filters for the individual columns.

    // As of HBase 0.94, the ColumnPaginationFilter, which we had been using to permit per-column
    // maxVersions settings, no longer pages over multiple versions for the same column. We can
    // still use it, however, to limit fully-qualified columns with maxVersions = 1 to return only
    // the most recent version in the request's time range. All other columns will use the largest
    // maxVersions seen on any column request.

    // Fortunately, although we may retrieve more versions per column than we need from HBase, we
    // can still honor the user's requested maxVersions when returning the versions in
    // HBaseFijiRowData.

    // Largest of the max-versions from all the requested columns.
    // Columns with paging are excluded (max-versions does not make sense when paging):
    int largestMaxVersions = 1;

    // If every column is paged, we should add a keyonly filter to a single column, so we can have
    // access to entityIds in our FijiRowData that is constructed.
    boolean completelyPaged = mFijiDataRequest.isPagingEnabled() ? true : false;

    for (FijiDataRequest.Column columnRequest : mFijiDataRequest.getColumns()) {
      final FijiColumnName fijiColumnName = columnRequest.getColumnName();
      final HBaseColumnName hbaseColumnName =
          mColumnNameTranslator.toHBaseColumnName(fijiColumnName);

      if (!columnRequest.isPagingEnabled()) {
        completelyPaged = false;

        // Do not include max-versions from columns with paging enabled:
        largestMaxVersions = Math.max(largestMaxVersions, columnRequest.getMaxVersions());
      }

      if (fijiColumnName.isFullyQualified()) {
        // Requests a fully-qualified column.
        // Adds this column to the Get request, and also as a filter.
        //
        // Filters are required here because we might end up requesting all cells from the
        // HBase family (ie. from the Fiji locality group), if a map-type family from that
        // locality group is also requested.
        addColumn(get, hbaseColumnName);
        columnFilters.addFilter(toFilter(columnRequest, hbaseColumnName, filterContext));

      } else {
        final FamilyLayout fLayout = tableLayout.getFamilyMap().get(fijiColumnName.getFamily());
        if (fLayout.isGroupType()) {
          // Requests all columns in a Fiji group-type family.
          // Expand the family request into individual column requests:
          for (String qualifier : fLayout.getColumnMap().keySet()) {
            final FijiColumnName fqFijiColumnName =
                FijiColumnName.create(fijiColumnName.getFamily(), qualifier);
            final HBaseColumnName fqHBaseColumnName =
                mColumnNameTranslator.toHBaseColumnName(fqFijiColumnName);
            addColumn(get, fqHBaseColumnName);
            columnFilters.addFilter(toFilter(columnRequest, fqHBaseColumnName, filterContext));
          }

        } else if (fLayout.isMapType()) {
          // Requests all columns in a Fiji map-type family.
          // We need to request all columns in the HBase family (ie. in the Fiji locality group)
          // and add a column prefix-filter to select only the columns from that Fiji family:
          get.addFamily(hbaseColumnName.getFamily());
          columnFilters.addFilter(toFilter(columnRequest, hbaseColumnName, filterContext));

        } else {
          throw new InternalFijiError("Family is neither group-type nor map-type");
        }
      }
    }

    if (completelyPaged) {
      // All requested columns have paging enabled.
      Preconditions.checkState(largestMaxVersions == 1);

      // We just need to know whether a row has data in at least one of the requested columns.
      // Stop at the first valid key using AND(columnFilters, FirstKeyOnlyFilter):
      get.setFilter(new FilterList(
          FilterList.Operator.MUST_PASS_ALL, columnFilters, new FirstKeyOnlyFilter()));
    } else {
      get.setFilter(columnFilters);
    }

    return get
        .setTimeRange(mFijiDataRequest.getMinTimestamp(), mFijiDataRequest.getMaxTimestamp())
        .setMaxVersions(largestMaxVersions);
  }

  /**
   * Adds a fully-qualified column to an HBase Get request, if necessary.
   *
   * <p>
   *   If the entire HBase family is already requested, the column does not need to be added.
   * </p>
   *
   * @param get Adds the column to this Get request.
   * @param column Fully-qualified HBase column to add to the Get request.
   * @return the Get request.
   */
  private static Get addColumn(final Get get, final HBaseColumnName column) {
    // Calls to Get.addColumn() invalidate previous calls to Get.addFamily(),
    // so we only do it if:
    //   1. No data from the family has been added to the request yet,
    // OR
    //   2. Only specific columns from the family have been requested so far.
    // Note: the Get family-map uses null values to indicate requests for an entire HBase family.
    if (!get.familySet().contains(column.getFamily())
        || (get.getFamilyMap().get(column.getFamily()) != null)) {
      get.addColumn(column.getFamily(), column.getQualifier());
    }
    return get;
  }

  /**
   * Configures a Scan with the options specified on HBaseScanOptions.
   * Whenever an option is not specified on <code>scanOptions</code>,
   * the hbase default will be used instead.
   *
   * @param scan The Scan to configure.
   * @param scanOptions The options to configure this Scan with.
   */
  private void configureScan(final Scan scan, final HBaseScanOptions scanOptions) {
    if (null != scanOptions.getClientBufferSize()) {
      scan.setBatch(scanOptions.getClientBufferSize());
    }
    if (null != scanOptions.getServerPrefetchSize()) {
      scan.setCaching(scanOptions.getServerPrefetchSize());
    }
    if (null != scanOptions.getCacheBlocks()) {
      scan.setCacheBlocks(scanOptions.getCacheBlocks());
    }
  }

  /**
   * Constructs and returns the HBase filter that returns only the
   * data in a given Fiji column request.
   *
   * @param columnRequest A fiji column request.
   * @param hbaseColumnName HBase column name.
   * @param filterContext Context to translate Fiji column filters to HBase filters.
   * @return An HBase filter that retrieves only the data for the column request.
   * @throws IOException If there is an error.
   */
  private static Filter toFilter(
      final FijiDataRequest.Column columnRequest,
      final HBaseColumnName hbaseColumnName,
      final FijiColumnFilter.Context filterContext
  ) throws IOException {

    final FijiColumnName fijiColumnName = columnRequest.getColumnName();

    // Builds an HBase filter for the specified column:
    //     (HBase-family = Fiji-locality-group)
    // AND (HBase-qualifier = Fiji-family:qualifier / prefixed by Fiji-family:)
    // AND (ColumnPaginationFilter(limit=1))  // when paging or if max-versions is 1
    // AND (custom user filter)
    // AND (FirstKeyOnlyFilter)  // only when paging
    //
    // Note:
    //     We cannot use KeyOnlyFilter as this filter uses Filter.transform() which applies
    //     unconditionally on all the KeyValue in the HBase Result.
    final FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    // Only let cells from the locality-group (ie. HBase family) the column belongs to, ie:
    //     HBase-family = Fiji-locality-group
    filter.addFilter(SchemaPlatformBridge.get().createFamilyFilter(
        CompareFilter.CompareOp.EQUAL,
        hbaseColumnName.getFamily()));

    if (fijiColumnName.isFullyQualified()) {
      // Only let cells from the fully-qualified column ie.:
      //     HBase-qualifier = Fiji-family:qualifier
      filter.addFilter(SchemaPlatformBridge.get().createQualifierFilter(
          CompareFilter.CompareOp.EQUAL,
          hbaseColumnName.getQualifier()));
    } else {
      // Only let cells from the map-type family ie.:
      //     HBase-qualifier starts with "Fiji-family:"
      filter.addFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
    }

    if (columnRequest.isPagingEnabled()
        || (fijiColumnName.isFullyQualified() && (columnRequest.getMaxVersions() == 1))) {
      // For fully qualified columns where maxVersions = 1, we can use the
      // ColumnPaginationFilter to restrict the number of versions returned to at most 1.
      //
      // Other columns' maxVersions will be filtered client-side in HBaseFijiRowData.
      //
      // Prior to HBase 0.94, we could use this optimization for all fully qualified
      // columns' maxVersions requests, due to different behavior in the
      // ColumnPaginationFilter.
      //
      // Note: we could also use this for a map-type family if max-versions == 1,
      //     by setting limit = Integer.MAX_VALUE.
      final int limit = 1;
      final int offset = 0;
      filter.addFilter(new ColumnPaginationFilter(limit, offset));
    }

    // Add the optional user-specified column filter, if specified:
    if (columnRequest.getFilter() != null) {
      filter.addFilter(
          columnRequest.getFilter().toHBaseFilter(fijiColumnName, filterContext));
    }

    // If column has paging enabled, we just want to know about the existence of a cell:
    if (columnRequest.isPagingEnabled()) {
      filter.addFilter(new FirstKeyOnlyFilter());

      // TODO(SCHEMA-334) KeyOnlyFilter uses Filter.transform() which applies unconditionally.
      //     There is a chance that Filter.transform() may apply conditionally in the future,
      //     in which case we may re-introduce the KeyOnlyFilter.
      //     An alternative is to provide a custom HBase filter to handle Fiji data requests
      //     efficiently.
    }

    return filter;
  }

  /**
   * A Context for FijiColumnFilters that translates column names to their HBase
   * representation.
   */
  public static final class NameTranslatingFilterContext extends FijiColumnFilter.Context {
    /** The translator to use. */
    private final HBaseColumnNameTranslator mTranslator;

    /**
     * Initialize this context with the specified column name translator.
     *
     * @param translator the translator to use.
     */
    public NameTranslatingFilterContext(final HBaseColumnNameTranslator translator) {
      mTranslator = translator;
    }

    /** {@inheritDoc} */
    @Override
    public HBaseColumnName getHBaseColumnName(
        final FijiColumnName fijiColumnName
    ) throws NoSuchColumnException {
      return mTranslator.toHBaseColumnName(fijiColumnName);
    }
  }
}
