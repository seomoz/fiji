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

package com.moz.fiji.schema;

import java.util.Comparator;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.apache.avro.Schema;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * FijiCell represents a cell in a Fiji table.
 *
 * It contains the family, qualifier, timestamp
 * that uniquely locates the cell within a table as well as the data in the cell.
 *
 * <p>This class has a Java type parameter <code>T</code>, which should be the Java type
 * determined by the Avro Schema.  The mapping between Avro Schemas and Java types are
 * documented in the Avro Java API documentation:
 *
 * @param <T> Type of data stored in the cell.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class FijiCell<T> {
  private static final Comparator<FijiCell<?>> KEY_COMPARATOR = new KeyComparator();

  /**
   * Create a new FijiCell from the given coordinates and decoded value.
   *
   * @param column Name of the column in which this cell exists.
   * @param timestamp Timestamp of this cell in milliseconds since the epoch.
   * @param decodedCell Decoded cell content.
   * @param <T> Type of the value of this cell.
   * @return a new FijiCell from the given coordinates and decoded value.
   */
  public static <T> FijiCell<T> create(
      final FijiColumnName column,
      final long timestamp,
      final DecodedCell<T> decodedCell
  ) {
    return new FijiCell<T>(column, timestamp, decodedCell);
  }

  /** Type of a Fiji cell. */
  public enum CellType {
    /** Cell encoded with Avro. */
    AVRO,

    /** Cell encoded as a counter. */
    COUNTER
  }

  private final FijiColumnName mColumn;
  private final long mTimestamp;
  private final DecodedCell<T> mDecodedCell;

  /**
   * Initializes a new FijiCell.
   *
   * @param column Name of the column in which this cell exists.
   * @param timestamp Timestamp of this cell in milliseconds since the epoch.
   * @param decodedCell Decoded cell content.
   */
  private FijiCell(
      final FijiColumnName column,
      final long timestamp,
      final DecodedCell<T> decodedCell
  ) {
    Preconditions.checkArgument(column.isFullyQualified(),
        "Cannot create a FijiCell without a fully qualified column. Found family: %s",
        column.getName());
    mColumn = column;
    mTimestamp = timestamp;
    mDecodedCell = decodedCell;
  }

  /**
   * Initializes a FijiCell.
   *
   * @param family Fiji column family name of the cell.
   * @param qualifier Fiji column qualifier name of the cell.
   * @param timestamp Timestamp the cell was written at, in ms since the Epoch.
   * @param decodedCell Decoded cell content.
   * @deprecated FijiCell constructor is deprecated. Please use factory method
   *     {@link #create(FijiColumnName, long, DecodedCell)}. This constructor will become private in
   *     the future.
   */
  @Deprecated
  public FijiCell(String family, String qualifier, long timestamp, DecodedCell<T> decodedCell) {
    mColumn = FijiColumnName.create(
        Preconditions.checkNotNull(family),
        Preconditions.checkNotNull(qualifier));
    mTimestamp = timestamp;
    mDecodedCell = Preconditions.checkNotNull(decodedCell);
  }

  /**
   * Get the name of the column in which this cell exists.
   *
   * @return the name of the column in which this cell exists.
   */
  public FijiColumnName getColumn() {
    return mColumn;
  }

  /**
   * @return the Fiji column family name of this cell.
   * @deprecated getFamily is deprecated. Please use {@link #getColumn()}. This method will be
   *     removed in the future.
   */
  @Deprecated
  public String getFamily() {
    return mColumn.getFamily();
  }

  /**
   * @return the Fiji column qualifier name of this cell.
   * @deprecated getQualifier is deprecated. Please use {@link #getColumn()}. This method will be
   *     removed in the future.
   */
  @Deprecated
  public String getQualifier() {
    return mColumn.getQualifier();
  }

  /**
   * Get the timestamp of this cell in milliseconds since the epoch.
   *
   * @return the timestamp of this cell in milliseconds since the epoch.
   */
  public long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Get the content of this cell.
   *
   * @return the content of this cell.
   */
  public T getData() {
    return mDecodedCell.getData();
  }

  /**
   * Get the Avro Schema used to decode this cell, or null for non-Avro values.
   *
   * @return the Avro Schema used to decode this cell, or null for non-Avro values.
   */
  public Schema getReaderSchema() {
    return mDecodedCell.getReaderSchema();
  }

  /**
   * Get the Avro Schema used to encode this cell, or null for non-Avro values.
   *
   * @return the Avro Schema used to encode this cell, or null for non-Avro values.
   */
  public Schema getWriterSchema() {
    return mDecodedCell.getWriterSchema();
  }

  /**
   * Get this cell's encoding type.
   *
   * @return this cell's encoding type.
   */
  public CellType getType() {
    if (mDecodedCell.getWriterSchema() == null) {
      return CellType.COUNTER;
    } else {
      return CellType.AVRO;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object object) {
    if (object instanceof FijiCell) {
      final FijiCell<?> that = (FijiCell<?>) object;
      return this.mColumn.equals(that.mColumn)
          && (this.mTimestamp == that.mTimestamp)
          && this.mDecodedCell.equals(that.mDecodedCell);
    } else {
      return false;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mColumn, mTimestamp, mDecodedCell);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(FijiCell.class)
        .add("column", mColumn.getName())
        .add("timestamp", mTimestamp)
        .add("encoding", getType())
        .add("content", getData())
        .toString();
  }

  /**
   * Get a comparator for {@link FijiCell}s which compares on the column and timestamp of cells.
   *
   * <p>
   *   Because the entity id is not included in {@code FijiCell}s, the comparator should not be
   *   used for comparing cells from different rows.
   * </p>
   *
   * @return a comparator for {@code FijiCell}s on column and timestamp.
   */
  public static Comparator<FijiCell<?>> getKeyComparator() {
    return KEY_COMPARATOR;
  }

  /**
   * A comparator for {@link FijiCell}s which compares on the column and timestamp.
   *
   * <p>
   *   Because the entity Id is not included in {@code FijiCell}s, this comparator should not be
   *   used for comparing cells from different rows.
   * </p>
   */
  private static class KeyComparator implements Comparator<FijiCell<?>> {
    /** {@inheritDoc} */
    @Override
    public int compare(final FijiCell<?> o1, final FijiCell<?> o2) {
      return ComparisonChain.start()
          .compare(o1.getColumn(), o2.getColumn())
          .compare(o2.getTimestamp(), o1.getTimestamp())
          .result();
    }
  }
}
