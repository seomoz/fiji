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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiColumnPagingNotEnabledException;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiPager;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.filter.Filters;
import com.moz.fiji.schema.filter.FijiColumnFilter;
import com.moz.fiji.schema.filter.FijiColumnRangeFilter;
import com.moz.fiji.schema.filter.StripValueColumnFilter;
import com.moz.fiji.schema.impl.FijiPaginationFilter;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.Debug;

/**
 * HBase implementation of FijiPager for map-type families.
 *
 * <p>
 *   This pager lists the qualifiers in the map-type family and nothing else.
 *   In particular, the cells content is not retrieved.
 * </p>
 *
 * <p>
 *   This pager conforms to the FijiPager interface, in order to implement
 *   {@link FijiRowData#getPager(String)}.
 *   More straightforward interfaces are available using {@link HBaseQualifierPager} and
 *   {@link HBaseQualifierIterator}.
 * </p>
 *
 * @see HBaseQualifierPager
 * @see HBaseQualifierIterator
 */
@ApiAudience.Private
public final class HBaseMapFamilyPager implements FijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseMapFamilyPager.class);

  /** Entity ID of the row being paged through. */
  private final EntityId mEntityId;

  /** HBase FijiTable to read from. */
  private final HBaseFijiTable mTable;

  /** Name of the map-type family being paged through. */
  private final FijiColumnName mFamily;

  /** Full data request. */
  private final FijiDataRequest mDataRequest;

  /** Column data request for the map-type family to page through. */
  private final FijiDataRequest.Column mColumnRequest;

  /** The state of a map family pager instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this map family pager. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** True only if there is another page of data to read through {@link #next()}. */
  private boolean mHasNext;

  /**
   * Highest qualifier (according to the HBase bytes comparator) returned so far.
   * This is the low bound (exclusive) for qualifiers to retrieve next.
   */
  private String mMinQualifier = null;

  /**
   * Initializes a pager for a map-type family.
   *
   * <p>
   *   To get a pager for a column with paging enabled, use {@link FijiRowData#getPager(String)}.
   * </p>
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param table The Fiji table that this row belongs to.
   * @param family Iterate through the qualifiers from this map-type family.
   * @throws FijiColumnPagingNotEnabledException If paging is not enabled for the specified family.
   */
  HBaseMapFamilyPager(
      EntityId entityId,
      FijiDataRequest dataRequest,
      HBaseFijiTable table,
      FijiColumnName family)
      throws FijiColumnPagingNotEnabledException {

    Preconditions.checkArgument(!family.isFullyQualified(),
        "Must use HBaseQualifierPager on a map-type family, but got '{}'.", family);
    mFamily = family;

    mDataRequest = dataRequest;
    mColumnRequest = mDataRequest.getColumn(family.getFamily(), null);
    if (!mColumnRequest.isPagingEnabled()) {
      throw new FijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column [%s].", family));
    }

    mEntityId = entityId;
    mTable = table;
    mHasNext = true;  // there might be no page to read, but we don't know until we issue an RPC

    // Only retain the table if everything else ran fine:
    mTable.retain();

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open MapFamilyPager instance in state %s.", oldState);
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot check has next while MapFamilyPager is in state %s.", state);
    return mHasNext;
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowData next() {
    return next(mColumnRequest.getPageSize());
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowData next(int pageSize) {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get next while MapFamilyPager is in state %s.", state);
    if (!mHasNext) {
      throw new NoSuchElementException();
    }
    Preconditions.checkArgument(pageSize > 0, "Page size must be >= 1, got %s", pageSize);

    // Clone the column data request template, adjusting the filters to restrict the range
    // and the number of qualifiers to fetch.

    final FijiColumnFilter filter = Filters.and(
        new FijiColumnRangeFilter(mMinQualifier, false, null, false),  // qualifier > mMinQualifier
        mColumnRequest.getFilter(),  // user filter
        new FijiPaginationFilter(pageSize),  // Select at most one version / qualifier.
        new StripValueColumnFilter());  // discard the cell content, we just need the qualifiers

    final FijiDataRequest nextPageDataRequest = FijiDataRequest.builder()
        .withTimeRange(mDataRequest.getMinTimestamp(), mDataRequest.getMaxTimestamp())
        .addColumns(ColumnsDef.create()
            .withFilter(filter)
            .withMaxVersions(1)  // HBase pagination filter forces max-versions to 1
            .add(mFamily))
        .build();

    LOG.debug("HBaseMapPager data request: {} and page size {}", nextPageDataRequest, pageSize);

    final FijiTableLayout layout = mTable.getLayout();
    final HBaseColumnNameTranslator translator = HBaseColumnNameTranslator.from(layout);
    final HBaseDataRequestAdapter adapter =
        new HBaseDataRequestAdapter(nextPageDataRequest, translator);
    try {
      final Get hbaseGet = adapter.toGet(mEntityId, layout);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending HBase Get: {} with filter {}",
            hbaseGet, Debug.toDebugString(hbaseGet.getFilter()));
      }
      final Result result = doHBaseGet(hbaseGet);
      LOG.debug("Got {} cells over {} requested", result.size(), pageSize);

      final FijiRowData page =
          // No cell is being decoded here so we don't need a cell decoder provider:
          new HBaseFijiRowData(mTable, nextPageDataRequest, mEntityId, result, null);

      // There is an HBase bug that leads to less KeyValue being returned than expected.
      // An empty result appears to be a reliable way to detect the end of the iteration.
      if (result.isEmpty()) {
        mHasNext = false;
      } else {
        // Update the low qualifier bound for the next iteration:
        mMinQualifier = page.getQualifiers(mFamily.getFamily()).last();
      }

      return page;

    } catch (IOException ioe) {
      throw new FijiIOException(ioe);
    }
  }

  /**
   * Sends an HBase Get request.
   *
   * @param get HBase Get request.
   * @return the HBase Result.
   * @throws IOException on I/O error.
   */
  private Result doHBaseGet(Get get) throws IOException {
    final HTableInterface htable = mTable.openHTableConnection();
    try {
      return htable.get(get);
    } finally {
      htable.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("FijiPager.remove() is not supported.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close MapFamilyPager while in state %s", oldState);
    mTable.release();
  }
}
