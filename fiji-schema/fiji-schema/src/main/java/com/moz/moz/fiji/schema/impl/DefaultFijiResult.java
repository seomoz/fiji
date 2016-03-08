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
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiResult;

/**
 * A {@link FijiResult} which proxies all calls to a pair of paged and materialized Fiji results.
 *
 * @param <T> The type of {@code FijiCell} values in the view.
 */
@NotThreadSafe
@ApiAudience.Private
public final class DefaultFijiResult<T> implements FijiResult<T> {

  private final FijiDataRequest mDataRequest;
  private final FijiResult<T> mMaterializedResult;
  private final FijiResult<T> mPagedResult;

  /**
   * Construct a new {@code DefaultFijiResult} with the provided data request, paged fiji result,
   * and materialized fiji result.
   *
   * @param dataRequest The data request for the result.
   * @param materializedResult The materialized result.
   * @param pagedResult The paged result.
   */
  private DefaultFijiResult(
      final FijiDataRequest dataRequest,
      final FijiResult<T> materializedResult,
      final FijiResult<T> pagedResult
  ) {
    mDataRequest = dataRequest;
    mMaterializedResult = materializedResult;
    mPagedResult = pagedResult;
  }

  /**
   * Create a new {@code DefaultFijiResult} with the provided data request, paged result,
   * and materialized result.
   *
   * @param dataRequest The data request for the result.
   * @param materializedResult The materialized result.
   * @param pagedResult The paged result.
   * @param <T> The type of {@code FijiCell} values in the result.
   * @return A {@code FijiResult} wrapping the provided materialized and paged results.
   */
  public static <T> FijiResult<T> create(
      final FijiDataRequest dataRequest,
      final FijiResult<T> materializedResult,
      final FijiResult<T> pagedResult
  ) {
    return new DefaultFijiResult<T>(dataRequest, materializedResult, pagedResult);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mPagedResult.getEntityId();
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<FijiCell<T>> iterator() {
    return Iterables.concat(mMaterializedResult, mPagedResult).iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> FijiResult<U> narrowView(final FijiColumnName column) {
    final FijiDataRequest narrowRequest = narrowRequest(column, mDataRequest);

    if (narrowRequest.isEmpty()) {
      return new EmptyFijiResult<U>(mMaterializedResult.getEntityId(), narrowRequest);
    }

    boolean containsPagedColumns = false;
    boolean containsUnpagedColumns = false;
    for (Column columnRequest : narrowRequest.getColumns()) {
      if (columnRequest.isPagingEnabled()) {
        containsPagedColumns = true;
      } else {
        containsUnpagedColumns = true;
      }
      if (containsPagedColumns && containsUnpagedColumns) {
        return DefaultFijiResult.create(
            narrowRequest,
            mMaterializedResult.<U>narrowView(column),
            mPagedResult.<U>narrowView(column));
      }
    }

    if (containsPagedColumns) {
      return mPagedResult.narrowView(column);
    } else {
      return mMaterializedResult.narrowView(column);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    mPagedResult.close();
  }

  // -----------------------------------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Narrow a {@link FijiDataRequest} to a column.  Will return a new data request. The column may
   * be fully qualified or a family.
   *
   * @param column to narrow data request.
   * @param dataRequest to narrow.
   * @return a data request narrowed to the specified column.
   */
  public static FijiDataRequest narrowRequest(
      final FijiColumnName column,
      final FijiDataRequest dataRequest
  ) {
    final List<Column> columnRequests = getColumnRequests(column, dataRequest);

    final FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());
    for (Column columnRequest : columnRequests) {
      builder.newColumnsDef(columnRequest);
    }

    return builder.build();
  }

  /**
   * Retrieve the column requests corresponding to a Fiji column in a {@code FijiDataRequest}.
   *
   * <p>
   * If the requested column is fully qualified, and the request contains a family request
   * containing the column, a new {@code Column} request will be created which corresponds to
   * the requested family narrowed to the qualifier.
   * </p>
   *
   * @param column a fully qualified {@link FijiColumnName}
   * @param dataRequest the data request to get column request from.
   * @return the column request.
   */
  private static List<Column> getColumnRequests(
      final FijiColumnName column,
      final FijiDataRequest dataRequest
  ) {
    final Column exactRequest = dataRequest.getColumn(column);
    if (exactRequest != null) {
      return ImmutableList.of(exactRequest);
    }

    if (column.isFullyQualified()) {
      // The column is fully qualified, but a request doesn't exist for the qualified column.
      // Check if the family is requested, and if so create a new qualified-column request from it.
      final Column familyRequest =
          dataRequest.getRequestForColumn(FijiColumnName.create(column.getFamily(), null));
      if (familyRequest == null) {
        return ImmutableList.of();
      }
      ColumnsDef columnDef = ColumnsDef
          .create()
          .withFilter(familyRequest.getFilter())
          .withPageSize(familyRequest.getPageSize())
          .withMaxVersions(familyRequest.getMaxVersions())
          .add(column.getFamily(), column.getQualifier(), familyRequest.getReaderSpec());

      return ImmutableList.of(
          FijiDataRequest.builder().addColumns(columnDef).build().getColumn(column));
    } else {
      // The column is a family, but a request doesn't exist for the entire family add all requests
      // for individual columns in the family.
      ImmutableList.Builder<Column> columnRequests = ImmutableList.builder();
      for (Column columnRequest : dataRequest.getColumns()) {
        if (columnRequest.getColumnName().getFamily().equals(column.getFamily())) {
          columnRequests.add(columnRequest);
        }
      }
      return columnRequests.build();
    }
  }
}
