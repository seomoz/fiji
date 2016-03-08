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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * A {@link FijiResult} backed by a map of {@code FijiColumnName} to
 * {@code List&lt;FijiCell&lt;T&gt;&gt;}.
 *
 * @param <T> The type of {@code FijiCell} values in the view.
 */
@ApiAudience.Private
public final class MaterializedFijiResult<T> implements FijiResult<T> {
  private final EntityId mEntityId;
  private final FijiDataRequest mDataRequest;
  private final SortedMap<FijiColumnName, List<FijiCell<T>>> mColumns;

  /**
   * Create a {@code MaterializedFijiResult}.
   *
   * @param entityId The entity ID of the row containing this result.
   * @param dataRequest The Fiji data request which defines the columns in this result.
   * @param columns The materialized results.
   */
  private MaterializedFijiResult(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final SortedMap<FijiColumnName, List<FijiCell<T>>> columns
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mColumns = columns;
  }

  /**
   * Create a new materialized {@code FijiResult} backed by the provided {@code FijiCell}s.
   *
   * @param entityId The entity ID of the row containing this result.
   * @param dataRequest The Fiji data request which defines the columns in this result.
   * @param layout The Fiji table layout of the table.
   * @param columns The materialized results. The cells must be in the order guaranteed by
   *     {@code FijiResult}. Must be mutable, and should not be modified after passing in.
   * @param <T> The type of {@code FijiCell} values in the view.
   * @return A new materialized {@code FijiResult} backed by {@code FijiCell}s.
   */
  public static <T> MaterializedFijiResult<T> create(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final FijiTableLayout layout,
      final SortedMap<FijiColumnName, List<FijiCell<T>>> columns
  ) {
    // We have to sort the results from group-type column families because the FijiResult API does
    // not specify the order of columns in a group-type family.  We rely on the columns being
    // ordered so that we can use binary search.

    final List<Map.Entry<FijiColumnName, List<FijiCell<T>>>> groupFamilyEntries =
        Lists.newArrayListWithCapacity(columns.size());
    for (Map.Entry<FijiColumnName, List<FijiCell<T>>> entry : columns.entrySet()) {
      final FijiColumnName column = entry.getKey();
      if (!column.isFullyQualified()
          && layout.getFamilyMap().get(column.getFamily()).isGroupType()) {
        groupFamilyEntries.add(entry);
      }
    }

    if (groupFamilyEntries.isEmpty()) {
      return new MaterializedFijiResult<T>(entityId, dataRequest, columns);
    }

    for (Map.Entry<FijiColumnName, List<FijiCell<T>>> entry : groupFamilyEntries) {
      final FijiColumnName groupFamily = entry.getKey();
      final List<FijiCell<T>> sortedColumn = entry.getValue();
      Collections.sort(sortedColumn, FijiCell.getKeyComparator());
      columns.put(groupFamily, sortedColumn);
    }

    return new MaterializedFijiResult<T>(entityId, dataRequest, columns);
  }

  /**
   * Create a new materialized {@code FijiResult} backed by the provided {@code FijiCell}s.
   *
   * @param entityId The entity ID of the row containing this result.
   * @param dataRequest The Fiji data request which defines the columns in this result.
   * @param columns The materialized results. The cells must be in the order guaranteed by
   *     {@code FijiResult}. Must be mutable, and should not be modified after passing in.
   * @param <T> The type of {@code FijiCell} values in the view.
   * @return A new materialized {@code FijiResult} backed by {@code FijiCell}s.
   * @deprecated This version is deprecated in favor of the four argument version, which
   * requires a FijiTableLayout parameter. This version is currently only in use by
   * Fiji-Spark.
   */
  @Deprecated
  public static <T> MaterializedFijiResult<T> create(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final SortedMap<FijiColumnName, List<FijiCell<T>>> columns
  ) {
    return new MaterializedFijiResult<T>(entityId, dataRequest, columns);
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
    return Iterables.concat(mColumns.values()).iterator();
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <U extends T> FijiResult<U> narrowView(final FijiColumnName column) {
    final FijiDataRequest narrowRequest = DefaultFijiResult.narrowRequest(column, mDataRequest);
    if (narrowRequest.equals(mDataRequest)) {
      return (FijiResult<U>) this;
    }

    final ImmutableSortedMap.Builder<FijiColumnName, List<FijiCell<U>>> narrowedColumns =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : narrowRequest.getColumns()) {
      final FijiColumnName requestedColumn = columnRequest.getColumnName();

      // (Object) cast is necessary. Might be: http://bugs.java.com/view_bug.do?bug_id=6548436
      final List<FijiCell<U>> exactColumn =
          (List<FijiCell<U>>) (Object) mColumns.get(requestedColumn);
      if (exactColumn != null) {

        // We get here IF

        // `column` is a family, and `mDataRequest` contains a column request for the entire family.

        // OR

        // `column` is a family, and `mDataRequest` contains a column request for a qualified column
        // in the family.

        // OR

        // `column` is a qualified-column, and `mDataRequest` contains a request for the qualified
        // column.

        narrowedColumns.put(requestedColumn, exactColumn);
      } else {

        // `column` is a qualified-column, and `mDataRequest` contains a column request for the
        // column's family.

        final List<FijiCell<T>> familyCells =
            mColumns.get(FijiColumnName.create(requestedColumn.getFamily(), null));
        final List<FijiCell<T>> qualifierCells = getQualifierCells(requestedColumn, familyCells);
        narrowedColumns.put(requestedColumn, (List<FijiCell<U>>) (Object) qualifierCells);
      }
    }

    return new MaterializedFijiResult<U>(
        mEntityId,
        narrowRequest,
        narrowedColumns.build());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // No-op
  }

  /**
   * Get the {@code FijiCell}s in a list of cells from a family which belong to the specified
   * fully-qualified column.
   *
   * @param column The fully-qualified column to retrieve cells for.
   * @param familyCells The sorted list of cells belonging to a Fiji family.
   * @return The {@code Cell}s for the qualified column.
   * @param <T> The type of {@code FijiCell}s in the family.
   */
  private static <T> List<FijiCell<T>> getQualifierCells(
      final FijiColumnName column,
      final List<FijiCell<T>> familyCells
  ) {
    if (familyCells.size() == 0) {
      return ImmutableList.of();
    }

    final FijiCell<T> start = FijiCell.create(column, Long.MAX_VALUE, null);
    final FijiCell<T> end = FijiCell.create(column, -1L, null);

    int startIndex =
        Collections.binarySearch(familyCells, start, FijiCell.getKeyComparator());
    if (startIndex < 0) {
      startIndex = -1 - startIndex;
    }

    int endIndex =
        Collections.binarySearch(familyCells, end, FijiCell.getKeyComparator());
    if (endIndex < 0) {
      endIndex = -1 - endIndex;
    }

    return familyCells.subList(startIndex, endIndex);
  }
}
