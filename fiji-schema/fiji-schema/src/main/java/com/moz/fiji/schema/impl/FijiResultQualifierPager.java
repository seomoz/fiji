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

package com.moz.fiji.schema.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.DecodedCell;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiPager;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * A Fiji qualifier pager backed by a {@link FijiResult}.
 */
@ApiAudience.Private
public class FijiResultQualifierPager implements FijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(FijiResultQualifierPager.class);

  private final FijiResult<Object> mResult;
  private final PeekingIterator<FijiCell<Object>> mCells;
  private final Column mColumnRequest;
  private final FijiTableLayout mLayout;
  private String mLastQualifier = null;

  /**
   * Create a Fiji qualifier pager backed by a {@code FijiResult}.
   *
   * @param result The {@code FijiResult} backing this pager.
   * @param layout The {@code FijiTableLayout} of the table.
   */
  public FijiResultQualifierPager(
      final FijiResult<Object> result,
      final FijiTableLayout layout
  ) {
    mResult = result;
    mCells = Iterators.peekingIterator(mResult.iterator());
    mLayout = layout;

    final FijiDataRequest dataRequest = mResult.getDataRequest();
    final Collection<Column> columnRequests = dataRequest.getColumns();
    Preconditions.checkArgument(columnRequests.size() == 1,
        "Can not create FijiResultPager with multiple columns. Data request: %s.", dataRequest);
    mColumnRequest = columnRequests.iterator().next();
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowData next() {
    return next(mColumnRequest.getPageSize());
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowData next(final int pageSize) {
    if (!hasNext()) {
      throw new NoSuchElementException("Fiji qualifier pager is exhausted.");
    }
    final FijiColumnName column = mColumnRequest.getColumnName();
    final ColumnsDef columnDef = ColumnsDef
        .create()
        .withFilter(mColumnRequest.getFilter())
        .withPageSize(FijiDataRequest.PAGING_DISABLED)
        .withMaxVersions(mColumnRequest.getMaxVersions())
        .add(column, mColumnRequest.getReaderSpec());
    final FijiDataRequestBuilder dataRequest = FijiDataRequest.builder();
    dataRequest.addColumns(columnDef);

    final List<FijiCell<Object>> cells = Lists.newArrayListWithCapacity(pageSize);

    while (mCells.hasNext() && cells.size() < pageSize) {
      final FijiCell<Object> cell = mCells.next();
      final String qualifier = cell.getColumn().getQualifier();
      if (!qualifier.equals(mLastQualifier)) {
        cells.add(
            FijiCell.create(
                cell.getColumn(),
                cell.getTimestamp(),
                new DecodedCell<Object>(cell.getWriterSchema(), null)));
        mLastQualifier = qualifier;
      }
    }

    final FijiResult<Object> result = MaterializedFijiResult.create(
        mResult.getEntityId(),
        dataRequest.build(),
        mLayout,
        ImmutableSortedMap.<FijiColumnName, List<FijiCell<Object>>>naturalOrder()
            .put(column, cells)
            .build());
    return new FijiResultRowData(mLayout, result);
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("FijiPager does not support remove.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mResult.close();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    while (mCells.hasNext() && mCells.peek().getColumn().getQualifier().equals(mLastQualifier)) {
      mCells.next();
    }
    return mCells.hasNext();
  }
}
