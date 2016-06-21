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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiCellDecoder;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiColumnPagingNotEnabledException;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiPager;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTableReaderBuilder;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.impl.BoundColumnReaderSpec;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;
import com.moz.fiji.schema.platform.SchemaPlatformBridge;
import com.moz.fiji.schema.util.TimestampComparator;

/**
 * An implementation of FijiRowData that wraps an HBase Result object.
 */
@ApiAudience.Private
public final class HBaseFijiRowData implements FijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFijiRowData.class);

  /** The entity id for the row. */
  private final EntityId mEntityId;

  /** The request used to retrieve this Fiji row data. */
  private final FijiDataRequest mDataRequest;

  /** The HBase FijiTable we are reading from. */
  private final HBaseFijiTable mTable;

  /** The layout for the table this row data came from. */
  private final FijiTableLayout mTableLayout;

  /** The HBase result providing the data of this object. */
  private Result mResult;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mDecoderProvider;

  /** A map from fiji family to fiji qualifier to timestamp to raw encoded cell values. */
  private NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> mFilteredMap;

  /**
   * Creates a provider for cell decoders.
   *
   * <p> The provider creates decoders for specific Avro records. </p>
   *
   * @param table HBase FijiTable to create a CellDecoderProvider for.
   * @return a new CellDecoderProvider for the specified HBase FijiTable.
   * @throws IOException on I/O error.
   */
  private static CellDecoderProvider createCellProvider(
      final HBaseFijiTable table
  ) throws IOException {
    return CellDecoderProvider.create(
        table.getLayout(),
        ImmutableMap.<FijiColumnName, BoundColumnReaderSpec>of(),
        ImmutableList.<BoundColumnReaderSpec>of(),
        FijiTableReaderBuilder.DEFAULT_CACHE_MISS);
  }

  /**
   * Initializes a row data from an HBase Result.
   *
   * <p>
   *   The HBase Result may contain more cells than are requested by the user.
   *   FijiDataRequest is more expressive than HBase Get/Scan requests.
   *   Currently, {@link #getMap()} attempts to complete the filtering to meet the data request
   *   requirements expressed by the user, but this can be inaccurate.
   * </p>
   *
   * @param table Fiji table containing this row.
   * @param dataRequest Data requested for this row.
   * @param entityId This row entity ID.
   * @param result HBase result containing the requested cells (and potentially more).
   * @param decoderProvider Provider for cell decoders.
   *     Null means the row creates its own provider for cell decoders (not recommended).
   * @throws IOException on I/O error.
   */
  public HBaseFijiRowData(
      final HBaseFijiTable table,
      final FijiDataRequest dataRequest,
      final EntityId entityId,
      final Result result,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    mTable = table;
    mTableLayout = table.getLayout();
    mDataRequest = dataRequest;
    mEntityId = entityId;
    mResult = result;
    mDecoderProvider = ((decoderProvider != null) ? decoderProvider : createCellProvider(table))
        .getDecoderProviderForRequest(dataRequest);
  }

  /**
   * An iterator for cells in group type column or map type column family.
   *
   * @param <T> The type parameter for the FijiCells being iterated over.
   */
  private static final class FijiCellIterator<T> implements Iterator<FijiCell<T>> {

    private static final KVComparator KV_COMPARATOR = new KVComparator();

    /**
     * Finds the insertion point of the pivot KeyValue in the KeyValue array and returns the index.
     *
     * @param kvs The KeyValue array to search in.
     * @param pivotKeyValue A KeyValue that is less than or equal to the first KeyValue for our
     *     column, and larger than any KeyValue that may preceed values for our desired column.
     * @return The index of the first KeyValue in the desired map type family.
     */
    private static int findInsertionPoint(final KeyValue[] kvs, final KeyValue pivotKeyValue) {
      // Now find where the pivotKeyValue would be placed
      int binaryResult = Arrays.binarySearch(kvs, pivotKeyValue, KV_COMPARATOR);
      if (binaryResult < 0) {
        return -1 - binaryResult; // Algebra on the formula provided in the binary search JavaDoc.
      } else {
        return binaryResult;
      }
    }

    private final KeyValue[] mKeyValues;
    private final EntityId mEntityId;
    private final FijiColumnName mColumn;
    private final FijiCellDecoder<T> mDecoder;
    private final HBaseColumnNameTranslator mColumnNameTranslator;
    private final int mMaxVersions;
    private int mCurrentVersions = 0;
    private int mNextIndex = 0;
    private FijiCell<T> mNextCell = null;

    /**
     * Create a new FijiCellIterator.
     *
     * @param rowData FijiRowData from which to retrieve cells.
     * @param columnName Column across which to iterate. May be a fully qualified column or map type
     *     family.
     * @param columnNameTranslator HBaseColumnNameTranslator with which to decode KeyValues.
     * @throws IOException In case of an error initializing the Iterator.
     */
    private FijiCellIterator(
        final HBaseFijiRowData rowData,
        final FijiColumnName columnName,
        final HBaseColumnNameTranslator columnNameTranslator
    ) throws IOException {
      mKeyValues = SchemaPlatformBridge.get().keyValuesFromResult(rowData.mResult);
      mEntityId = rowData.mEntityId;
      mColumn = columnName;
      mDecoder = rowData.mDecoderProvider.getDecoder(mColumn);
      mColumnNameTranslator = columnNameTranslator;
      mMaxVersions = rowData.mDataRequest.getRequestForColumn(mColumn).getMaxVersions();

      mNextIndex = findStartIndex();
      mNextCell = getNextCell();
    }

    /**
     * Find the start index of the configured column in the KeyValues of this Iterator.
     *
     * @return the start index of the configured column in the KeyValues of this Iterator.
     * @throws NoSuchColumnException in case the column does not exist in the table.
     */
    private int findStartIndex() throws NoSuchColumnException {
      final HBaseColumnName hBaseColumnName = mColumnNameTranslator.toHBaseColumnName(mColumn);
      final KeyValue kv = new KeyValue(
          mEntityId.getHBaseRowKey(),
          hBaseColumnName.getFamily(),
          hBaseColumnName.getQualifier(),
          Long.MAX_VALUE,
          new byte[0]);
      return findInsertionPoint(mKeyValues, kv);
    }

    /**
     * Get the next cell to be returned by this Iterator. Null indicates the iterator is exhausted.
     *
     * @return the next cell to be returned by this Iterator or null if the iterator is exhausted.
     *
     * @throws IOException in case of an error decoding the cell.
     */
    private FijiCell<T> getNextCell() throws IOException {
      // Ensure that we do not attempt to get KeyValues from out of bounds indices.
      if (mNextIndex >= mKeyValues.length) {
        return null;
      }

      final KeyValue next = mKeyValues[mNextIndex];
      final HBaseColumnName hbaseColumn = new HBaseColumnName(
          next.getFamily(),
          next.getQualifier());
      final FijiColumnName column = mColumnNameTranslator.toFijiColumnName(hbaseColumn);

      // Validates that the column of the next KeyValue should be included in the iterator.
      if (mColumn.isFullyQualified()) {
        if (!Objects.equal(mColumn, column)) {
          // The column of the next cell is not the requested column, do not return it.
          return null;
        }
      } else {
        if (!Objects.equal(column.getFamily(), mColumn.getFamily())) {
          // The column of the next cell is not in the requested family, do not return it.
          return null;
        }
      }

      if ((null != mNextCell) && !Objects.equal(mNextCell.getColumn(), column)) {
        // We've hit the next qualifier before the max versions; reset the current version count.
        mCurrentVersions = 0;
      }

      if (mCurrentVersions < mMaxVersions) {
        // decode the cell and return it.
        mCurrentVersions++;
        mNextIndex++;
        return FijiCell.create(column, next.getTimestamp(), mDecoder.decodeCell(next.getValue()));
      } else {
        // Reset the current versions and try the next qualifier.
        mCurrentVersions = 0;
        final KeyValue nextQualifierKV = new KeyValue(
            mEntityId.getHBaseRowKey(),
            hbaseColumn.getFamily(),
            Arrays.copyOf(hbaseColumn.getQualifier(), hbaseColumn.getQualifier().length + 1),
            Long.MAX_VALUE,
            new byte[0]);
        mNextIndex = findInsertionPoint(mKeyValues, nextQualifierKV);
        return getNextCell();
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return (null != mNextCell);
    }

    /** {@inheritDoc} */
    @Override
    public FijiCell<T> next() {
      final FijiCell<T> next = mNextCell;
      if (null == next) {
        throw new NoSuchElementException();
      } else {
        try {
          mNextCell = getNextCell();
        } catch (IOException ioe) {
          throw new FijiIOException(ioe);
        }
        return next;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          String.format("%s does not support remove().", getClass().getName()));
    }
  }

  /**
   * An iterable of cells in a column.
   *
   * @param <T> The type parameter for the FijiCells being iterated over.
   */
  private static final class CellIterable<T> implements Iterable<FijiCell<T>> {
    /** The column family. */
    private final FijiColumnName mColumnName;
    /** The rowdata we are iterating over. */
    private final HBaseFijiRowData mRowData;
    /** The HBaseColumnNameTranslator for the column. */
    private final HBaseColumnNameTranslator mHBaseColumnNameTranslator;

    /**
     * An iterable of FijiCells, for a particular column.
     *
     * @param colName The Fiji column family that is being iterated over.
     * @param rowdata The HBaseFijiRowData instance containing the desired data.
     * @param columnNameTranslator of the table we are iterating over.
     */
    protected CellIterable(
        final FijiColumnName colName,
        final HBaseFijiRowData rowdata,
        final HBaseColumnNameTranslator columnNameTranslator
    ) {
      mColumnName = colName;
      mRowData = rowdata;
      mHBaseColumnNameTranslator = columnNameTranslator;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<FijiCell<T>> iterator() {
      try {
        return new FijiCellIterator<T>(
            mRowData,
            mColumnName,
            mHBaseColumnNameTranslator);
      } catch (IOException ex) {
        throw new FijiIOException(ex);
      }
    }
  }

  /**
   * Gets the HBase result backing this {@link com.moz.fiji.schema.FijiRowData}.
   *
   * @return The HBase result.
   */
  public Result getHBaseResult() {
    return mResult;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /**
   * Gets the table this row data belongs to.
   *
   * @return the table this row data belongs to.
   */
  public HBaseFijiTable getTable() {
    return mTable;
  }

  /**
   * Gets the data request used to retrieve this row data.
   *
   * @return the data request used to retrieve this row data.
   */
  public FijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /**
   * Gets the layout of the table this row data belongs to.
   *
   * @return The table layout.
   */
  public FijiTableLayout getTableLayout() {
    return mTableLayout;
  }

  /**
   * Gets a map from fiji family to qualifier to timestamp to raw fiji-encoded bytes of a cell.
   *
   * @return The map.
   */
  public synchronized NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>
      getMap() {
    if (null != mFilteredMap) {
      return mFilteredMap;
    }

    LOG.debug("Filtering the HBase Result into a map of fiji cells...");
    final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map =
        mResult.getMap();
    mFilteredMap = new TreeMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>();
    if (null == map) {
      LOG.debug("No result data.");
      return mFilteredMap;
    }

    final HBaseColumnNameTranslator columnNameTranslator =
        HBaseColumnNameTranslator.from(mTableLayout);
    // Loop over the families in the HTable.
    for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry
             : map.entrySet()) {
      // Loop over the columns in the family.
      for (NavigableMap.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry
               : familyEntry.getValue().entrySet()) {
        final HBaseColumnName hbaseColumnName =
            new HBaseColumnName(familyEntry.getKey(), columnEntry.getKey());

        // Translate the HBase column name to a Fiji column name.
        FijiColumnName fijiColumnName;
        try {
          fijiColumnName = columnNameTranslator.toFijiColumnName(
              new HBaseColumnName(familyEntry.getKey(), columnEntry.getKey()));
        } catch (NoSuchColumnException e) {
          LOG.info("Ignoring HBase column '{}:{}' because it doesn't contain Fiji data.",
              Bytes.toStringBinary(hbaseColumnName.getFamily()),
              Bytes.toStringBinary(hbaseColumnName.getQualifier()));
          continue;
        }
        LOG.debug("Adding family [{}] to getMap() result.", fijiColumnName.getName());

        // First check if all columns were requested.
        FijiDataRequest.Column columnRequest =
            mDataRequest.getColumn(fijiColumnName.getFamily(), null);
        if (null == columnRequest) {
          // Not all columns were requested, so check if this particular column was.
          columnRequest =
              mDataRequest.getColumn(fijiColumnName.getFamily(), fijiColumnName.getQualifier());
        }
        if (null == columnRequest) {
          LOG.debug("Ignoring unrequested data: " + fijiColumnName.getFamily() + ":"
              + fijiColumnName.getQualifier());
          continue;
        }

        // Loop over the versions.
        int numVersions = 0;
        for (NavigableMap.Entry<Long, byte[]> versionEntry : columnEntry.getValue().entrySet()) {
          if (numVersions >= columnRequest.getMaxVersions()) {
            LOG.debug("Skipping remaining cells because we hit max versions requested: "
                + columnRequest.getMaxVersions());
            break;
          }

          // Read the timestamp.
          final long timestamp = versionEntry.getKey();
          if (mDataRequest.isTimestampInRange(timestamp)) {
            // Add the cell to the filtered map.
            if (!mFilteredMap.containsKey(fijiColumnName.getFamily())) {
              mFilteredMap.put(fijiColumnName.getFamily(),
                  new TreeMap<String, NavigableMap<Long, byte[]>>());
            }
            final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap =
                mFilteredMap.get(fijiColumnName.getFamily());
            if (!columnMap.containsKey(fijiColumnName.getQualifier())) {
              columnMap.put(fijiColumnName.getQualifier(),
                  new TreeMap<Long, byte[]>(TimestampComparator.INSTANCE));
            }
            final NavigableMap<Long, byte[]> versionMap =
                columnMap.get(fijiColumnName.getQualifier());
            versionMap.put(versionEntry.getKey(), versionEntry.getValue());
            ++numVersions;
          } else {
            LOG.debug("Excluding cell at timestamp " + timestamp + " because it is out of range ["
                + mDataRequest.getMinTimestamp() + "," + mDataRequest.getMaxTimestamp() + ")");
          }
        }
      }
    }
    return mFilteredMap;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(final String family, final String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
    final NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
    if (null == versionMap) {
      return false;
    }
    return !versionMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(final String family) {

    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
    for (Map.Entry<String, NavigableMap<String, NavigableMap<Long, byte[]>>> columnMapEntry
      : getMap().entrySet()) {
      LOG.debug("The result return contains family [{}]", columnMapEntry.getKey());
    }
    return !columnMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsCell(
      final String family,
      final String qualifier,
      long timestamp
  ) {
    return containsColumn(family, qualifier)
        && getTimestamps(family, qualifier).contains(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<String> getQualifiers(final String family) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return Sets.newTreeSet();
    }
    return qmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<Long> getTimestamps(
      final String family,
      final String qualifier
  ) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return Sets.newTreeSet(TimestampComparator.INSTANCE);
    }
    return tmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(final String family, final String qualifier) throws IOException {
    return mTableLayout.getCellSpec(FijiColumnName.create(family, qualifier)).getAvroSchema();
  }

  /**
   * Reports the encoded map of qualifiers of a given family.
   *
   * @param family Family to look up.
   * @return the encoded map of qualifiers in the specified family, or null.
   */
  private NavigableMap<String, NavigableMap<Long, byte[]>> getRawQualifierMap(final String family) {
    return getMap().get(family);
  }

  /**
   * Reports the specified raw encoded time-series of a given column.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @return the encoded time-series in the specified family:qualifier column, or null.
   */
  private NavigableMap<Long, byte[]> getRawTimestampMap(
      final String family,
      final String qualifier
  ) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return null;
    }
    return qmap.get(qualifier);
  }

  /**
   * Reports the encoded content of a given cell.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @param timestamp Timestamp to look up.
   * @return the encoded cell content, or null.
   */
  private byte[] getRawCell(String family, String qualifier, long timestamp) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    return tmap.get(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(String family, String qualifier, long timestamp) throws IOException {
    final FijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(FijiColumnName.create(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException {
    final FijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(FijiColumnName.create(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return FijiCell.create(
        FijiColumnName.create(family, qualifier),
        timestamp,
        decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    final FijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(FijiColumnName.create(family, qualifier));
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(String family) throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentValues(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentValues(String family, String qualifier) method.",
        family);
    final NavigableMap<String, T> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final T value = getMostRecentValue(family, qualifier);
      result.put(qualifier, value);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, T> getValues(String family, String qualifier)
      throws IOException {
    final NavigableMap<Long, T> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, FijiCell<T>> entry : this.<T>getCells(family, qualifier).entrySet()) {
      result.put(entry.getKey(), entry.getValue().getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getValues(String family) is only enabled on map "
        + "type column families. The column family [%s], is a group type column family. Please use "
        + "the getValues(String family, String qualifier) method.",
        family);
    final NavigableMap<String, NavigableMap<Long, T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, T> timeseries = getValues(family, qualifier);
      result.put(qualifier, timeseries);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiCell<T> getMostRecentCell(String family, String qualifier) throws IOException {
    final FijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(FijiColumnName.create(family, qualifier));
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    final long timestamp = tmap.firstKey();
    return FijiCell.create(
        FijiColumnName.create(family, qualifier),
        timestamp,
        decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, FijiCell<T>> getMostRecentCells(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentCells(String family, String qualifier) method.",
        family);
    final NavigableMap<String, FijiCell<T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final FijiCell<T> cell = getMostRecentCell(family, qualifier);
      result.put(qualifier, cell);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, FijiCell<T>> getCells(String family, String qualifier)
      throws IOException {
    final FijiCellDecoder<T> decoder =
        mDecoderProvider.getDecoder(FijiColumnName.create(family, qualifier));

    final NavigableMap<Long, FijiCell<T>> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (tmap != null) {
      for (Map.Entry<Long, byte[]> entry : tmap.entrySet()) {
        final Long timestamp = entry.getKey();
        final byte[] bytes = entry.getValue();
        final FijiCell<T> cell =
            FijiCell.create(
                FijiColumnName.create(family, qualifier),
                timestamp,
                decoder.decodeCell(bytes));
        result.put(timestamp, cell);
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, FijiCell<T>>> getCells(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getCells(String family, String qualifier) method.",
        family);
    final NavigableMap<String, NavigableMap<Long, FijiCell<T>>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, FijiCell<T>> cells = getCells(family, qualifier);
      result.put(qualifier, cells);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<FijiCell<T>> iterator(String family, String qualifier)
      throws IOException {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new FijiCellIterator<T>(this, column, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<FijiCell<T>> iterator(String family)
      throws IOException {
    final FijiColumnName column = FijiColumnName.create(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "iterator(String family) is only enabled"
            + " on map type column families. The column family [%s], is a group type column family."
            + " Please use the iterator(String family, String qualifier) method.",
        family);
    return new FijiCellIterator<T>(this, column, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<FijiCell<T>> asIterable(String family, String qualifier) {
    final FijiColumnName column = FijiColumnName.create(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new CellIterable<T>(column, this, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<FijiCell<T>> asIterable(String family) {
    final FijiColumnName column = FijiColumnName.create(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "asIterable(String family) is only enabled"
            + " on map type column families. The column family [%s], is a group type column family."
            + " Please use the asIterable(String family, String qualifier) method.",
        family);
    return new CellIterable<T>(column, this, mTable.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public FijiPager getPager(String family, String qualifier)
      throws FijiColumnPagingNotEnabledException {
    final FijiColumnName fijiColumnName = FijiColumnName.create(family, qualifier);
    return new HBaseVersionPager(
        mEntityId, mDataRequest, mTable,  fijiColumnName, mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public FijiPager getPager(String family) throws FijiColumnPagingNotEnabledException {
    final FijiColumnName fijiFamily = FijiColumnName.create(family, null);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getPager(String family) is only enabled on map type column families. "
        + "The column family '%s' is a group type column family. "
        + "Please use the getPager(String family, String qualifier) method.",
        family);
    return new HBaseMapFamilyPager(mEntityId, mDataRequest, mTable, fijiFamily);
  }

  /**
   * Get a FijiResult corresponding to the same data as this KjiRowData.
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code FijiCell}s
   *   of the returned {@code FijiResult}. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code FijiResult} is
   *   used. See the 'Type Safety' section of {@link FijiResult}'s documentation for more details.
   * </p>
   *
   * @return A FijiResult corresponding to the same data as this FijiRowData.
   * @param <T> The type of {@code FijiCell} values in the returned {@code FijiResult}.
   * @throws IOException if error while decoding cells.
   */
  public <T> FijiResult<T> asFijiResult() throws IOException {
    return HBaseFijiResult.create(
        mEntityId,
        mDataRequest,
        mResult,
        mTable,
        mTableLayout,
        HBaseColumnNameTranslator.from(mTableLayout),
        mDecoderProvider);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseFijiRowData.class)
        .add("table", mTable.getURI())
        .add("entityId", getEntityId())
        .add("dataRequest", mDataRequest)
        .add("resultSize", mResult.size())
        .add("result", mResult)
        .add("map", getMap())
        .toString();
  }
}
