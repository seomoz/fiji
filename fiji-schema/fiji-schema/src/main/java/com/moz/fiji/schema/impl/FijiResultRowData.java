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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiColumnPagingNotEnabledException;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiPager;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiResult.Helpers;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.util.TimestampComparator;

/**
 * A {@code FijiRowData} implementation backed by a {@code FijiResult}.
 */
@ApiAudience.Private
public class FijiResultRowData implements FijiRowData {
  private final FijiTableLayout mLayout;
  private final FijiResult<Object> mResult;

  /**
   * Constructor for {@code FijiResultRowData}.
   *
   * @param layout The {@code FijiTableLayout} for the table.
   * @param result The {@code FijiResult} backing this {@code FijiRowData}.
   */
  public FijiResultRowData(final FijiTableLayout layout, final FijiResult<Object> result) {
    mLayout = layout;
    mResult = result;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mResult.getEntityId();
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsColumn(final String family, final String qualifier) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    return containsColumnRequest(column) && mResult.narrowView(column).iterator().hasNext();
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsColumn(final String family) {
    return containsColumn(family, null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsCell(final String family, final String qualifier, final long timestamp) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    return containsColumnRequest(column) && getCell(family, qualifier, timestamp) != null;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableSet<String> getQualifiers(final String family) {
    final FijiColumnName column = FijiColumnName.create(family, null);
    validateColumnRequest(column);
    final NavigableSet<String> qualifiers = Sets.newTreeSet();
    for (final FijiCell<?> cell : mResult.narrowView(column)) {
      qualifiers.add(cell.getColumn().getQualifier());
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableSet<Long> getTimestamps(final String family, final String qualifier) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    validateColumnRequest(column);
    final NavigableSet<Long> timestamps = Sets.newTreeSet();
    for (final FijiCell<?> cell : mResult.narrowView(column)) {
      timestamps.add(cell.getTimestamp());
    }
    return timestamps;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(final String family, final String qualifier) throws IOException {
    return mLayout.getCellSpec(FijiColumnName.create(family, qualifier)).getAvroSchema();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(final String family, final String qualifier, final long timestamp) {
    final FijiCell<T> cell = getCell(family, qualifier, timestamp);
    return cell == null ? null : cell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(final String family, final String qualifier) {
    final FijiCell<T> cell = getMostRecentCell(family, qualifier);
    return cell == null ? null : cell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(final String family) {
    final FijiColumnName column = FijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentValues(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getMostRecentValues(String family, String qualifier) method.",
        family);

    final NavigableMap<String, T> qualifiers = Maps.newTreeMap();
    final Collection<Column> columnRequests = mResult.getDataRequest().getColumns();

    final Column familyRequest = mResult.getDataRequest().getRequestForColumn(family, null);
    if (familyRequest != null) {
      String previousQualifier = null;
      final FijiResult<T> narrowView = mResult.narrowView(column);
      for (final FijiCell<T> cell : narrowView) {
        final String qualifier = cell.getColumn().getQualifier();
        if (!qualifier.equals(previousQualifier)) {
          qualifiers.put(qualifier, cell.getData());
          previousQualifier = qualifier;
        }
      }
    } else {
      final List<FijiColumnName> requestedColumns = Lists.newArrayList();
      for (final Column columnRequest : columnRequests) {
        if (columnRequest.getFamily().equals(family)) {
          requestedColumns.add(columnRequest.getColumnName());
        }
      }

      for (final FijiColumnName requestedColumn : requestedColumns) {
        final FijiCell<T> cell = Helpers.getFirst(mResult.<T>narrowView(requestedColumn));
        if (cell != null) {
          qualifiers.put(cell.getColumn().getQualifier(), cell.getData());
        }
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(final String family) {
    final FijiColumnName column = FijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getValues(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getValues(String family, String qualifier) method.", family);

    final NavigableMap<String, NavigableMap<Long, T>> qualifiers = Maps.newTreeMap();
    for (final FijiCell<T> cell : mResult.<T>narrowView(column)) {
      NavigableMap<Long, T> columnValues = qualifiers.get(cell.getColumn().getQualifier());
      if (columnValues == null) {
        columnValues = Maps.newTreeMap(TimestampComparator.INSTANCE);
        qualifiers.put(cell.getColumn().getQualifier(), columnValues);
      }
      columnValues.put(cell.getTimestamp(), cell.getData());
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, T> getValues(final String family, final String qualifier) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    final NavigableMap<Long, T> values = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (final FijiCell<T> cell : mResult.<T>narrowView(column)) {
      values.put(cell.getTimestamp(), cell.getData());
    }
    return values;
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiCell<T> getCell(
      final String family,
      final String qualifier,
      final long version
  ) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    validateColumnRequest(column);
    final FijiResult<T> columnView = mResult.narrowView(column);
    for (final FijiCell<T> cell : columnView) {
      final long cellVersion = cell.getTimestamp();
      if (cellVersion == version) {
        return cell;
      } else if (cellVersion < version) {
        break;
      }
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiCell<T> getMostRecentCell(final String family, final String qualifier) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    validateColumnRequest(column);
    return Helpers.getFirst(mResult.<T>narrowView(column));
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, FijiCell<T>> getMostRecentCells(final String family) {
    final FijiColumnName column = FijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentCells(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getMostRecentCells(String family, String qualifier) method.",
        family);

    final NavigableMap<String, FijiCell<T>> qualifiers = Maps.newTreeMap();
    final Collection<Column> columnRequests = mResult.getDataRequest().getColumns();

    final Column familyRequest = mResult.getDataRequest().getRequestForColumn(family, null);
    if (familyRequest != null) {
      String previousQualifier = null;
      for (final FijiCell<T> cell : mResult.<T>narrowView(column)) {
        final String qualifier = cell.getColumn().getQualifier();
        if (!qualifier.equals(previousQualifier)) {
          qualifiers.put(qualifier, cell);
          previousQualifier = qualifier;
        }
      }
    } else {
      final List<FijiColumnName> requestedColumns = Lists.newArrayList();
      for (final Column columnRequest : columnRequests) {
        if (columnRequest.getFamily().equals(family)) {
          requestedColumns.add(columnRequest.getColumnName());
        }
      }

      for (final FijiColumnName requestedColumn : requestedColumns) {
        final FijiCell<T> cell = Helpers.getFirst(mResult.<T>narrowView(requestedColumn));
        if (cell != null) {
          qualifiers.put(cell.getColumn().getQualifier(), cell);
        }
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, FijiCell<T>>> getCells(final String family) {
    final FijiColumnName column = FijiColumnName.create(family, null);
    validateColumnRequest(column);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getCells(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the getCells(String family, String qualifier) method.", family);

    final NavigableMap<String, NavigableMap<Long, FijiCell<T>>> qualifiers = Maps.newTreeMap();
    for (final FijiCell<T> cell : mResult.<T>narrowView(column)) {
      NavigableMap<Long, FijiCell<T>> columnValues =
          qualifiers.get(cell.getColumn().getQualifier());
      if (columnValues == null) {
        columnValues = Maps.newTreeMap(TimestampComparator.INSTANCE);
        qualifiers.put(cell.getColumn().getQualifier(), columnValues);
      }
      columnValues.put(cell.getTimestamp(), cell);
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, FijiCell<T>> getCells(final String family, final String qualifier) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    final NavigableMap<Long, FijiCell<T>> cells = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (final FijiCell<T> cell : mResult.<T>narrowView(column)) {
      cells.put(cell.getTimestamp(), cell);
    }
    return cells;
  }

  /** {@inheritDoc} */
  @Override
  public FijiPager getPager(
      final String family,
      final String qualifier
  ) throws FijiColumnPagingNotEnabledException {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);

    final Column columnRequest = mResult.getDataRequest().getRequestForColumn(column);

    Preconditions.checkNotNull(columnRequest,
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());

    if (!columnRequest.isPagingEnabled()) {
      throw new FijiColumnPagingNotEnabledException(
          String.format("Requested column %s does not have paging enabled in data request %s.",
          column, mResult.getDataRequest()));
    }

    return new FijiResultPager(mResult.narrowView(column), mLayout);
  }

  /** {@inheritDoc} */
  @Override
  public FijiPager getPager(final String family) throws FijiColumnPagingNotEnabledException {
    final FijiColumnName column = FijiColumnName.create(family, null);
    Preconditions.checkState(mLayout.getFamilyMap().get(family).isMapType(),
        "getPager(String family) is only enabled on map type column families. "
            + "The column family '%s' is a group type column family. "
            + "Please use the getPager(String family, String qualifier) method.", family);

    final Column columnRequest = mResult.getDataRequest().getColumn(column);
    Preconditions.checkNotNull(columnRequest,
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());

    if (!columnRequest.isPagingEnabled()) {
      throw new FijiColumnPagingNotEnabledException(
          String.format("Requested column %s does not have paging enabled in data request %s.",
              column, mResult.getDataRequest()));
    }

    return new FijiResultQualifierPager(mResult.narrowView(column), mLayout);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<FijiCell<T>> iterator(final String family, final String qualifier) {
    return this.<T>asIterable(family, qualifier).iterator();
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<FijiCell<T>> iterator(final String family) {
    final FamilyLayout familyLayout = mLayout.getFamilyMap().get(family);
    Preconditions.checkArgument(familyLayout != null, "Column %s has no data request.", family);
    Preconditions.checkState(familyLayout.isMapType(),
        "iterator(String family) is only enabled on map type column families."
            + " The column family [%s], is a group type column family."
            + " Please use the iterator(String family, String qualifier) method.", family);

    return iterator(family, null);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<FijiCell<T>> asIterable(
      final String family,
      final String qualifier
  ) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    Preconditions.checkArgument(containsColumnRequest(column),
        "Column %s has no data request.", column, mResult.getDataRequest());

    return mResult.narrowView(column);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<FijiCell<T>> asIterable(final String family) {
    return asIterable(family, null);
  }

  /**
   * Validates that the given {@code FijiColumnName} is requested in the {@code FijiDataRequest}
   * for this {@code FijiRowData}.  This means different things for different column types:
   *
   * <h4>Fully Qualified Column</h4>
   * <p>
   *   The column or its family must be in the data request.  The requested column or family may not
   *   be paged.
   * </p>
   *
   * <h4>Family Column</h4>
   * <p>
   *   The family, or at least one fully qualified column belonging to the family must be in the
   *   data request.  The family column request or the qualified column requests belonging to the
   *   family may not be paged.
   * </p>
   *
   * @param column The column for which to validate a request exists.
   */
  private void validateColumnRequest(final FijiColumnName column) {
    if (!containsColumnRequest(column)) {
      throw new NullPointerException(
          String.format("Requested column %s is not included in the data request %s.",
              column, mResult.getDataRequest()));
    }
  }

  /**
   * Returns whether the data request contains the column. This means different things for different
   * column types:
   *
   * <h4>Fully Qualified Column</h4>
   * <p>
   *   The column or its family must be in the data request.  The requested column or family may not
   *   be paged.
   * </p>
   *
   * <h4>Family Column</h4>
   * <p>
   *   The family, or at least one fully qualified column belonging to the family must be in the
   *   data request.  The family column request or the qualified column requests belonging to the
   *   family may not be paged.
   * </p>
   *
   * @param column The column for which to validate a request exists.
   * @return Whether the data request contains a request for the column.
   */
  private boolean containsColumnRequest(final FijiColumnName column) {
    final FijiDataRequest dataRequest = mResult.getDataRequest();
    final Column exactRequest = dataRequest.getColumn(column);
    if (exactRequest != null) {
      Preconditions.checkArgument(!exactRequest.isPagingEnabled(),
          "Paging is enabled for column %s in data request %s.", exactRequest, dataRequest);
      return true;
    } else if (column.isFullyQualified()) {
      // The column is fully qualified, but a request doesn't exist for the qualified column.
      // Check if the family is requested, and validate it.
      final Column familyRequest =
          dataRequest.getColumn(FijiColumnName.create(column.getFamily(), null));
      if (familyRequest == null) {
        return false;
      }
      Preconditions.checkArgument(
          !familyRequest.isPagingEnabled(),
          "Paging is enabled for column %s in data request %s.", familyRequest, dataRequest);
      return true;
    } else {
      boolean requestContainsColumns = false;
      for (final Column columnRequest : dataRequest.getColumns()) {
        if (columnRequest.getColumnName().getFamily().equals(column.getFamily())) {
          Preconditions.checkArgument(
              !columnRequest.isPagingEnabled(),
              "Paging is enabled for column %s in data request %s.", columnRequest, dataRequest);
          requestContainsColumns = true;
        }
      }
      return requestContainsColumns;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mLayout, mResult);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final FijiResultRowData other = (FijiResultRowData) obj;
    return Objects.equal(this.mLayout, other.mLayout) && Objects.equal(this.mResult, other.mResult);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mLayout", mLayout)
        .add("mResult", mResult)
        .toString();
  }
}
