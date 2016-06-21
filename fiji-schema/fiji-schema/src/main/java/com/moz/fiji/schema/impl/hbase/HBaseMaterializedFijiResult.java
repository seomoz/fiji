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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.impl.DefaultFijiResult;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * A {@link FijiResult} backed by an HBase {@link Result}.
 *
 * @param <T> The type of {@code FijiCell} values in the view.
 */
@ApiAudience.Private
public final class HBaseMaterializedFijiResult<T> implements FijiResult<T> {
  private final EntityId mEntityId;
  private final FijiDataRequest mDataRequest;
  private final FijiTableLayout mLayout;
  private final HBaseColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<FijiColumnName, List<KeyValue>> mColumnResults;

  /**
   * Create a {@code FijiResult} backed by an HBase {@link Result}.
   *
   * @param entityId The entity ID of the row to which the {@code Result} belongs.
   * @param dataRequest The data request which defines the columns in this {@code FijiResult}.
   * @param layout The Fiji table layout of the table.
   * @param columnTranslator The Fiji column name translator of the table.
   * @param decoderProvider The Fiji cell decoder provider of the table.
   * @param columnResults The materialized HBase results.
   */
  private HBaseMaterializedFijiResult(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final FijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider,
      final SortedMap<FijiColumnName, List<KeyValue>> columnResults
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;
    mColumnResults = columnResults;
  }

  /**
   * Create a {@code FijiResult} backed by an HBase {@link Result}.
   *
   * @param entityId The entity ID of the row to which the {@code Result} belongs.
   * @param dataRequest The data request which defines the columns in this {@code FijiResult}.
   * @param result The backing HBase result.
   * @param layout The Fiji table layout of the table.
   * @param columnTranslator The Fiji column name translator of the table.
   * @param decoderProvider The Fiji cell decoder provider of the table.
   * @param <T> The type of {@code FijiCell} values in the view.
   * @return A {@code FijiResult} backed by an HBase {@code Result}.
   */
  public static <T> HBaseMaterializedFijiResult<T> create(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final Result result,
      final FijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) {
    final ImmutableSortedMap.Builder<FijiColumnName, List<KeyValue>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : dataRequest.getColumns()) {
      // TODO: determine via benchmarks whether it would be faster to make a copy of the
      // columnResult list so that the underlying Result may be garbage collected.
      List<KeyValue> columnResult = getColumnKeyValues(columnRequest, columnTranslator, result);
      columnResults.put(columnRequest.getColumnName(), columnResult);
    }

    return new HBaseMaterializedFijiResult<T>(
        entityId,
        dataRequest,
        layout,
        columnTranslator,
        decoderProvider,
        columnResults.build());
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
    final List<Iterator<FijiCell<T>>> columnIterators =
        Lists.newArrayListWithCapacity(mColumnResults.size());

    for (Map.Entry<FijiColumnName, List<KeyValue>> entry : mColumnResults.entrySet()) {
      final Function<KeyValue, FijiCell<T>> decoder =
          ResultDecoders.getDecoderFunction(
              entry.getKey(),
              mLayout,
              mColumnTranslator,
              mDecoderProvider);

      columnIterators.add(Iterators.transform(entry.getValue().iterator(), decoder));
    }
    return Iterators.concat(columnIterators.iterator());
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> HBaseMaterializedFijiResult<U> narrowView(final FijiColumnName column) {
    final FijiDataRequest narrowRequest = DefaultFijiResult.narrowRequest(column, mDataRequest);
    if (narrowRequest.equals(mDataRequest)) {
      return (HBaseMaterializedFijiResult<U>) this;
    }

    final ImmutableSortedMap.Builder<FijiColumnName, List<KeyValue>> narrowedResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : narrowRequest.getColumns()) {
      final FijiColumnName requestColumnName = columnRequest.getColumnName();

      // We get here IF

      // `column` is a family, and `mDataRequest` contains a column request for the entire family.

      // OR

      // `column` is a family, and `mDataRequest` contains a column request for a qualified column
      // in the family.

      // OR

      // `column` is a qualified-column, and `mDataRequest` contains a request for the qualified
      // column.

      final List<KeyValue> exactColumn = mColumnResults.get(requestColumnName);
      if (exactColumn != null) {
        narrowedResults.put(requestColumnName, exactColumn);
      } else {

        // `column` is a qualified-column, and `mDataRequest` contains a column request for the
        // column's family.

        final List<KeyValue> familyResults =
            mColumnResults.get(FijiColumnName.create(requestColumnName.getFamily(), null));
        final List<KeyValue> qualifiedColumnResults =
            getQualifiedColumnKeyValues(columnRequest, mColumnTranslator, familyResults);

        narrowedResults.put(requestColumnName, qualifiedColumnResults);
      }
    }

    return new HBaseMaterializedFijiResult<U>(
        mEntityId,
        mDataRequest,
        mLayout,
        mColumnTranslator,
        mDecoderProvider,
        narrowedResults.build());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // No-op
  }

  // -----------------------------------------------------------------------------------------------
  // Static classes and helper methods
  // -----------------------------------------------------------------------------------------------

  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Get the list of {@code KeyValue}s in a {@code Result} belonging to a column request.
   *
   * <p>
   *   This method will filter extra version from the result if necessary.
   * </p>
   *
   * @param columnRequest of the column whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the column.
   */
  private static List<KeyValue> getColumnKeyValues(
      final Column columnRequest,
      final HBaseColumnNameTranslator translator,
      final Result result
  ) {
    if (result.isEmpty()) {
      return ImmutableList.of();
    }
    final FijiColumnName column = columnRequest.getColumnName();
    final List<KeyValue> keyValues = Arrays.asList(result.raw());

    if (column.isFullyQualified()) {
      return getQualifiedColumnKeyValues(columnRequest, translator, keyValues);
    } else {
      return getFamilyKeyValues(columnRequest, translator, keyValues);
    }
  }

  /**
   * Get the list of {@code KeyValue}s in the {@code Result} belonging to a fully-qualified column
   * request.
   *
   * <p>
   *   This method will filter extra versions from the result if the number of versions in the
   *   result is greater than the column's requested max versions.
   * </p>
   *
   * @param columnRequest of the column whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the qualified column.
   */
  private static List<KeyValue> getQualifiedColumnKeyValues(
      final Column columnRequest,
      final HBaseColumnNameTranslator translator,
      final List<KeyValue> result
  ) {
    if (result.size() == 0) {
      return ImmutableList.of();
    }
    final byte[] rowkey = result.get(0).getRow();
    final byte[] family;
    final byte[] qualifier;
    try {
      final HBaseColumnName hbaseColumn =
          translator.toHBaseColumnName(columnRequest.getColumnName());
      family = hbaseColumn.getFamily();
      qualifier = hbaseColumn.getQualifier();
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(e);
    }

    final KeyValue start = new KeyValue(rowkey, family, qualifier, Long.MAX_VALUE, EMPTY_BYTES);
    // HBase will never return a KeyValue with a negative timestamp, so -1 is fine for exclusive end
    final KeyValue end = new KeyValue(rowkey, family, qualifier, -1, EMPTY_BYTES);

    final List<KeyValue> columnKeyValues = getSublist(result, KeyValue.COMPARATOR, start, end);

    final int maxVersions = columnRequest.getMaxVersions();
    if (columnKeyValues.size() > maxVersions) {
      return columnKeyValues.subList(0, maxVersions);
    } else {
      return columnKeyValues;
    }
  }

  /**
   * Get the list of {@code KeyValue}s in a {@code Result} belonging to a familyRequest request.
   *
   * <p>
   *   This method will filter extra versions from each column if necessary.
   * </p>
   *
   * @param familyRequest The familyRequest whose {@code KeyValues} to return.
   * @param translator The column name translator for the table.
   * @param result The scan results.
   * @return the {@code KeyValue}s for the specified familyRequest.
   */
  private static List<KeyValue> getFamilyKeyValues(
      final Column familyRequest,
      final HBaseColumnNameTranslator translator,
      final List<KeyValue> result
  ) {
    final FijiColumnName column = familyRequest.getColumnName();
    if (result.size() == 0) {
      return ImmutableList.of();
    }
    final byte[] rowkey = result.get(0).getRow();
    final byte[] family;
    final byte[] qualifier;
    try {
      final HBaseColumnName hbaseColumn =
          translator.toHBaseColumnName(column);
      family = hbaseColumn.getFamily();
      qualifier = hbaseColumn.getQualifier();
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(e);
    }

    List<KeyValue> keyValues = Lists.newArrayList();

    final KeyValue familyStartKV =
        new KeyValue(rowkey, family, qualifier, Integer.MAX_VALUE, EMPTY_BYTES);

    // Index of the current qualified column index
    int columnStart = getElementIndex(result, 0, familyStartKV);
    while (columnStart < result.size()
        && column.getFamily().equals(getKeyValueColumnName(result.get(columnStart), translator)
            .getFamily())) {

      final KeyValue start = result.get(columnStart);
      final KeyValue end = new KeyValue(
          rowkey,
          start.getFamily(),
          start.getQualifier(),
          -1,
          EMPTY_BYTES);

      final int columnEnd = getElementIndex(result, columnStart, end);

      final int length = Math.min(columnEnd - columnStart, familyRequest.getMaxVersions());

      keyValues.addAll(result.subList(columnStart, columnStart + length));
      columnStart = columnEnd;
    }

    return keyValues;
  }

  /**
   * Get the FijiColumnName encoded in the Key of a given KeyValue.
   *
   * @param kv KeyValue from which to get the encoded FijiColumnName.
   * @param translator for table.
   * @return the FijiColumnName encoded in the Key of a given KeyValue.
   */
  private static FijiColumnName getKeyValueColumnName(
      final KeyValue kv,
      final HBaseColumnNameTranslator translator
  ) {
    final HBaseColumnName hBaseColumnName = new HBaseColumnName(kv.getFamily(), kv.getQualifier());
    try {
      return translator.toFijiColumnName(hBaseColumnName);
    } catch (NoSuchColumnException nsce) {
      // This should not happen since it's only called on data returned by HBase.
      throw new IllegalStateException(
          String.format("Unknown column name in KeyValue: %s.", kv));
    }
  }

  /**
   * Get the index of a {@code KeyValue} in an array.
   *
   * @param result to search for the {@code KeyValue}. Must be sorted.
   * @param start index to start search for the {@code KeyValue}.
   * @param element to search for.
   * @return the index that the element resides, or would reside if it were to be inserted into the
   *     sorted array.
   */
  private static int getElementIndex(
      final List<KeyValue> result,
      final int start,
      final KeyValue element
  ) {
    final List<KeyValue> sublist = result.subList(start, result.size());
    final int index = Collections.binarySearch(sublist, element, KeyValue.COMPARATOR);
    if (index < 0) {
      return (-1 - index) + start;
    } else {
      return index + start;
    }
  }

  /**
   * Get a sublist of a list starting at the {@code start} element (inclusive), and extending to the
   * {@code end} element (exclusive).
   *
   * @param list from which to take the sublist. Must be sorted.
   * @param comparator which the list is sorted with.
   * @param start element for the sublist (inclusive).
   * @param end element for the sublist (exclusive).
   * @param <T> The type of list element.
   * @return the sublist from the provided sorted list which contains the {@code start} element and
   *    excludes the {@code end} element.
   */
  private static <T> List<T> getSublist(
      final List<T> list,
      final Comparator<? super T> comparator,
      final T start,
      final T end
  ) {
    int startIndex = Collections.binarySearch(list, start, comparator);
    if (startIndex < 0) {
      startIndex = -1 - startIndex;
    }
    int endIndex = Collections.binarySearch(list, end, comparator);
    if (endIndex < 0) {
      endIndex = -1 - endIndex;
    }

    return list.subList(startIndex, endIndex);
  }
}
