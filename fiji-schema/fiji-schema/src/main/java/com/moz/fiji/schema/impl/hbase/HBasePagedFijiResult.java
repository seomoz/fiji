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

package com.moz.fiji.schema.impl.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.filter.FijiColumnFilter;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.impl.DefaultFijiResult;
import com.moz.fiji.schema.impl.hbase.HBaseDataRequestAdapter.NameTranslatingFilterContext;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * A {@link FijiResult} backed by on-demand HBase scans.
 *
 * <p>
 *   {@code HBasePagedKiijResult} is <em>not</em> thread safe.
 * </p>
 *
 * @param <T> The type of {@code FijiCell} values in the view.
 */
@ApiAudience.Private
public class HBasePagedFijiResult<T> implements FijiResult<T> {
  private final EntityId mEntityId;
  private final FijiDataRequest mDataRequest;
  private final HBaseFijiTable mTable;
  private final FijiTableLayout mLayout;
  private final HBaseColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<FijiColumnName, Iterable<FijiCell<T>>> mColumnResults;
  private final Closer mCloser;

  /**
   * This result does not need to be closed unless {@link #iterator()} is called, so we defer
   * registering with the debug resource tracker till that point. This variable keeps track of
   * whether we have registered yet. Does not need to be atomic, because this class is not thread
   * safe.
   */
  private boolean mDebugRegistered = false;

  /**
   * Create a new {@link HBasePagedFijiResult}.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest FijiDataRequest defining the values to retrieve.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   */
  public HBasePagedFijiResult(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final HBaseFijiTable table,
      final FijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;
    mTable = table;
    mCloser = Closer.create();

    final ImmutableSortedMap.Builder<FijiColumnName, Iterable<FijiCell<T>>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : mDataRequest.getColumns()) {
      final PagedColumnIterable columnIterable = new PagedColumnIterable(columnRequest);
      mCloser.register(columnIterable);
      columnResults.put(columnRequest.getColumnName(), columnIterable);
    }

    mColumnResults = columnResults.build();
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<FijiCell<T>> iterator() {
    if (!mDebugRegistered) {
      ResourceTracker.get().registerResource(this);
      mDebugRegistered = true;
    }
    return Iterables.concat(mColumnResults.values()).iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> HBasePagedFijiResult<U> narrowView(final FijiColumnName column) {
    final FijiDataRequest narrowRequest = DefaultFijiResult.narrowRequest(column, mDataRequest);

    return new HBasePagedFijiResult<>(
        mEntityId,
        narrowRequest,
        mTable,
        mLayout,
        mColumnTranslator,
        mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    try {
      mCloser.close();
    } finally {
      if (mDebugRegistered) {
        ResourceTracker.get().unregisterResource(this);
        mDebugRegistered = false;
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helper classes and methods
  // -----------------------------------------------------------------------------------------------

  /**
   * An iterable which starts an HBase scan for each requested iterator.
   */
  public final class PagedColumnIterable implements Iterable<FijiCell<T>>, Closeable {
    private final FijiColumnName mColumn;
    private final Scan mScan;
    private final Closer mCloser;

    /**
     * Creates an iterable which starts an HBase scan for each requested iterator.
     *
     * @param columnRequest of column to scan.
     */
    private PagedColumnIterable(final Column columnRequest) {
      mColumn = columnRequest.getColumnName();
      mCloser = Closer.create();

      try {
        final FijiColumnFilter.Context filterContext =
            new NameTranslatingFilterContext(mColumnTranslator);
        final FijiColumnFilter fijiFilter = columnRequest.getFilter();
        final Filter filter;
        if (fijiFilter != null) {
          filter = fijiFilter.toHBaseFilter(mColumn, filterContext);
        } else {
          filter = null;
        }

        final byte[] rowkey = mEntityId.getHBaseRowKey();
        mScan = new Scan(rowkey, Arrays.copyOf(rowkey, rowkey.length + 1));

        final HBaseColumnName hbaseColumn = mColumnTranslator.toHBaseColumnName(mColumn);

        if (mColumn.isFullyQualified()) {
          mScan.addColumn(hbaseColumn.getFamily(), hbaseColumn.getQualifier());
          mScan.setFilter(filter);
        } else {
          if (Arrays.equals(hbaseColumn.getQualifier(), new byte[0])) {
            // This can happen with the native translator
            mScan.addFamily(hbaseColumn.getFamily());
            mScan.setFilter(filter);
          } else if (hbaseColumn.getQualifier().length == 0) {
            mScan.addFamily(hbaseColumn.getFamily());
            mScan.setFilter(filter);
          }
          mScan.addFamily(hbaseColumn.getFamily());

          final Filter prefixFilter = new ColumnPrefixFilter(hbaseColumn.getQualifier());
          if (filter != null) {
            final FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
            filters.addFilter(prefixFilter);
            filters.addFilter(filter);
            mScan.setFilter(filters);
          } else {
            mScan.setFilter(prefixFilter);
          }
        }

        mScan.setMaxVersions(columnRequest.getMaxVersions());
        mScan.setTimeRange(mDataRequest.getMinTimestamp(), mDataRequest.getMaxTimestamp());
        mScan.setBatch(columnRequest.getPageSize());
      } catch (IOException e) {
        throw new FijiIOException(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<FijiCell<T>> iterator() {
      final HTableInterface htable;
      final ResultScanner scanner;
      try {
        htable = mTable.openHTableConnection();
        mCloser.register(htable);
        scanner = htable.getScanner(mScan);
        mCloser.register(scanner);
      } catch (IOException e) {
        throw new FijiIOException(e);
      }

      // Decoder functions are stateful, so they should not be shared among multiple iterators
      final Function<KeyValue, FijiCell<T>> decoder =
          ResultDecoders.getDecoderFunction(mColumn, mLayout, mColumnTranslator, mDecoderProvider);

      return
          Iterators.concat(
              Iterators.transform(
                  scanner.iterator(),
                  new Function<Result, Iterator<FijiCell<T>>>() {
                    @Override
                    public Iterator<FijiCell<T>> apply(final Result result) {
                      return Iterators.transform(Iterators.forArray(result.raw()), decoder);
                    }
                  }));
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mCloser.close();
    }
  }
}
