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
import java.util.SortedMap;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.impl.DefaultFijiResult;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * A paged {@link FijiResult} on a Cassandra Fiji table.
 *
 * @param <T> The value type of cells in the result.
 */
@ApiAudience.Private
public class CassandraPagedFijiResult<T> implements FijiResult<T> {
  private final EntityId mEntityId;
  private final FijiDataRequest mDataRequest;
  private final CassandraFijiTable mTable;
  private final FijiTableLayout mLayout;
  private final CassandraColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<FijiColumnName, Iterable<FijiCell<T>>> mColumnResults;

  /**
   * Create a materialized {@code FijiResult} for a get on a Cassandra Fiji table.
   *
   * @param entityId The entity ID of the row to get.
   * @param dataRequest The data request defining the columns to get. All columns must be non-paged.
   * @param table The Cassandra Fiji table.
   * @param layout The layout of the table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   */
  public CassandraPagedFijiResult(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final CassandraFijiTable table,
      final FijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mTable = table;
    mLayout = layout;
    mColumnTranslator = translator;
    mDecoderProvider = decoderProvider;

    final ImmutableSortedMap.Builder<FijiColumnName, Iterable<FijiCell<T>>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : mDataRequest.getColumns()) {
      final PagedColumnIterable columnIterable = new PagedColumnIterable(columnRequest);
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
    return Iterables.concat(mColumnResults.values()).iterator();
  }

  /** {@inheritDoc} */
  @Override
  public <U extends T> FijiResult<U> narrowView(final FijiColumnName column) {
    final FijiDataRequest narrowRequest = DefaultFijiResult.narrowRequest(column, mDataRequest);

    return new CassandraPagedFijiResult<U>(
        mEntityId,
        narrowRequest,
        mTable,
        mLayout,
        mColumnTranslator,
        mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException { }

  /**
   * An iterable which starts a Cassandra scan for each requested iterator.
   */
  private final class PagedColumnIterable implements Iterable<FijiCell<T>> {
    private final Column mColumnRequest;

    /**
     * Creates an iterable which creates Cassandra queries for each requested iterator.
     *
     * @param columnRequest of column to scan.
     */
    private PagedColumnIterable(final Column columnRequest) {
      mColumnRequest = columnRequest;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<FijiCell<T>> iterator() {
      return CassandraFijiResult.unwrapFuture(
          CassandraFijiResult.<T>getColumn(
              mTable,
              mEntityId,
              mColumnRequest,
              mDataRequest,
              mLayout,
              mColumnTranslator,
              mDecoderProvider));
    }
  }
}
