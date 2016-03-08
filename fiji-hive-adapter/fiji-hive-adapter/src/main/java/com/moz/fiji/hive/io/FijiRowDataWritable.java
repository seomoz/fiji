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

package com.moz.fiji.hive.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.utils.DataRequestOptimizer;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiColumnPagingNotEnabledException;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiPager;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.impl.hbase.HBaseFijiRowData;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.TimestampComparator;

/**
 * Writable version of the data stored within a FijiRowData.  Contains a subset of methods
 * which are necessary for the Hive adapter.
 */
public class FijiRowDataWritable implements Writable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FijiRowDataWritable.class);

  private static final NavigableMap<Long, FijiCellWritable> EMPTY_DATA = Maps.newTreeMap();

  private EntityIdWritable mEntityId;

  // Backing store of the cell data contained in this row expressed as Writables.
  private Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> mWritableData;

  // Decoded data to be used by clients.  Lazily initialized, since these objects can be created
  // expressly for serialization.
  private Map<FijiColumnName, NavigableMap<Long, Object>> mDecodedData;

  // Schema data required decoding Avro data within cells.
  private Map<FijiColumnName, Schema> mSchemas;

  private FijiRowData mRowData;

  private Map<String, FijiPager> mFijiQualifierPagers;
  private Map<FijiColumnName, FijiPager> mFijiCellPagers;

  // Reference to FijiTableReader necessary for creating qualifier pages
  private FijiTableReader mReader;
  private Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> mQualifierPageData;

  /**
   * Tracks whether this instance has been closed yet. This is necessary so we don't double close
   * FijiPager objects. This can be removed when SCHEMA-787 hits. This should only be mutated from
   * #close().
   */
  private final AtomicBoolean mIsOpen = new AtomicBoolean(true);

  /** Required so that this can be built by WritableFactories. */
  public FijiRowDataWritable() {
  }

  /**
   * Construct a FijiRowDataWritable from the Writable objects generated from Hive.
   *
   * @param entityIdWritable that maps to the row key.
   * @param writableData of column to timeseries data.
   */
  public FijiRowDataWritable(EntityIdWritable entityIdWritable,
      Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> writableData) {
    mEntityId = entityIdWritable;
    mWritableData = writableData;
  }

  /**
   * Constructs a FijiRowDataWritable from a existing FijiRowData.
   *
   * @param rowData the source of the fields to copy.
   * @param fijiTableReader to be used for fetching qualifier pages. Must be closed by caller.
   * @throws IOException if there is an error loading the table layout.
   */
  public FijiRowDataWritable(FijiRowData rowData, FijiTableReader fijiTableReader)
      throws IOException {
    Preconditions.checkArgument(rowData instanceof HBaseFijiRowData,
        "FijiRowData must be an instance of HBaseFijiRowData to read TableLayout information.");

    mEntityId = new EntityIdWritable(rowData.getEntityId());
    mWritableData = Maps.newHashMap();
    mSchemas = Maps.newHashMap();

    mRowData = rowData;
    HBaseFijiRowData hBaseFijiRowData = (HBaseFijiRowData) rowData;

    mReader = fijiTableReader;

    mFijiQualifierPagers = getFijiQualifierPagers(hBaseFijiRowData.getDataRequest());
    mQualifierPageData = Maps.newHashMap();

    // While this only contains the fully qualified cell pagers, it will get overwritten when we
    // are paging through qualifiers.
    mFijiCellPagers = getFijiCellPagers(hBaseFijiRowData.getDataRequest(), mRowData);

    for (FamilyLayout familyLayout : hBaseFijiRowData.getTableLayout().getFamilies()) {
      String family = familyLayout.getName();
      for (String qualifier : rowData.getQualifiers(family)) {
        FijiColumnName column = new FijiColumnName(family, qualifier);
        if (rowData.getCells(family, qualifier) != null) {
          NavigableMap<Long, FijiCellWritable> data =
              convertCellsToWritable(rowData.getCells(family, qualifier));

          mWritableData.put(column, data);

          Schema schema = rowData.getReaderSchema(family, qualifier);
          mSchemas.put(column, schema);
        }

        // If this column has cell paging, read in its schema
        if (mFijiCellPagers.containsKey(column)) {
          Schema schema = rowData.getReaderSchema(column.getFamily(), column.getQualifier());
          mSchemas.put(column, schema);
        }
      }

      // If this family has qualifier paging, read in its schema
      if (mFijiQualifierPagers.containsKey(family)) {
        FijiColumnName familyColumnName = new FijiColumnName(family);
        Schema schema = rowData.getReaderSchema(family, familyColumnName.getQualifier());
        mSchemas.put(familyColumnName, schema);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (mIsOpen.compareAndSet(true, false)) {
      for (FijiPager pager : mFijiQualifierPagers.values()) {
        ResourceUtils.closeOrLog(pager);
      }

      for (FijiPager pager : mFijiCellPagers.values()) {
        ResourceUtils.closeOrLog(pager);
      }
    }
  }

  /**
   * Checks the associated qualifier and cell pagers if there is additional paged data.
   *
   * @return whether there are more pages associated with this FijiRowDataWritable
   */
  public boolean hasMorePages() {
    for (FijiPager fijiQualifierPager : mFijiQualifierPagers.values()) {
      if (fijiQualifierPager.hasNext()) {
        return true;
      }
    }

    for (FijiPager fijiCellPager : mFijiCellPagers.values()) {
      if (fijiCellPager.hasNext()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Build the Writably compatible FijiRowDataPageWritable with the next page's data.  Contains
   * the logic for paging through qualifiers.  {@link #nextCellPage()} is used internally
   * to retrieve the results of the next page of cells.
   *
   * @return a FijiRowDataPageWritable with a page of data substituted.
   * @throws IOException if there was an error.
   */
  public FijiRowDataPageWritable nextPage() throws IOException {
    if (!mFijiCellPagers.isEmpty() && !mQualifierPageData.isEmpty()) {
      // If we are paging through cells and have cached qualifier page data,
      // return the next page of cells if it's not empty.
      FijiRowDataPageWritable nextPage = nextCellPage();
      if (!nextPage.isEmpty()) {
        return nextPage;
      }
    }

    // Start with empty cached qualifier page data.
    mQualifierPageData = Maps.newHashMap();

    // Build a set of a page worth of qualifiers for each qualifier pager.
    Set<FijiColumnName> qualifiersPage = Sets.newHashSet();
    for (Entry<String, FijiPager> pagerEntry : mFijiQualifierPagers.entrySet()) {
      String family = pagerEntry.getKey();
      FijiPager pager = pagerEntry.getValue();
      if (pager.hasNext()) {
        FijiRowData qualifierRowData = pager.next();
        NavigableSet<String> qualifiers = qualifierRowData.getQualifiers(family);
        for (String qualifier : qualifiers) {
          qualifiersPage.add(new FijiColumnName(family, qualifier));
        }
      }
    }

    // Assemble the cached qualifierPageData that is used to build up any resultant results
    if (!qualifiersPage.isEmpty()) {
      // Append paged qualifiers to the original FijiDataRequest
      HBaseFijiRowData hBaseFijiRowData = (HBaseFijiRowData) mRowData;
      FijiDataRequest originalDataRequest = hBaseFijiRowData.getDataRequest();
      FijiDataRequest fijiDataRequest =
          DataRequestOptimizer.expandFamilyWithPagedQualifiers(originalDataRequest, qualifiersPage);
      FijiRowData qualifierPage = mReader.get(mRowData.getEntityId(), fijiDataRequest);

      Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> qualifierPageData =
          Maps.newHashMap();
      for (FijiColumnName fijiColumnName : qualifiersPage) {
        final NavigableMap<Long, FijiCell<Object>> pagedData =
            qualifierPage.getCells(fijiColumnName.getFamily(), fijiColumnName.getQualifier());
        final NavigableMap<Long, FijiCellWritable> writableData =
            convertCellsToWritable(pagedData);
        qualifierPageData.put(fijiColumnName, writableData);
      }
      mQualifierPageData = qualifierPageData;
      if (mFijiCellPagers != null) {
        // Cleanup existing pagers
        for (FijiPager pager : mFijiCellPagers.values()) {
          ResourceUtils.closeOrLog(pager);
        }
      }
      mFijiCellPagers = getFijiCellPagers(fijiDataRequest, qualifierPage);
    }

    // Return the first result that's built up from a page of cells.
    return nextCellPage();
  }

  /**
   * Build the Writably compatible FijiRowDataPageWritable with the data for the next page of
   * cells.  If we aren't paging through any cells, then this will just return the data cached
   * from the qualifiers.
   *
   * @return a FijiRowDataPageWritable with a page of data substituted.
   * @throws IOException if there was an error.
   */
  private FijiRowDataPageWritable nextCellPage() throws IOException {
    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> pageData = Maps.newHashMap();

    // Add in all of the data from paged qualifiers
    pageData.putAll(mQualifierPageData);

    for (Map.Entry<FijiColumnName, FijiPager> entry : mFijiCellPagers.entrySet()) {
      final FijiColumnName fijiColumnName = entry.getKey();
      final FijiPager cellPager = entry.getValue();
      try {
        final FijiRowData pagedFijiRowData = cellPager.next();
        final NavigableMap<Long, FijiCell<Object>> pagedData =
            pagedFijiRowData.getCells(fijiColumnName.getFamily(), fijiColumnName.getQualifier());
        final NavigableMap<Long, FijiCellWritable> writableData = convertCellsToWritable(pagedData);
        pageData.put(fijiColumnName, writableData);
      } catch (NoSuchElementException nsee) {
        // If we run out of pages, put in a blank entry
        pageData.put(fijiColumnName, EMPTY_DATA);
      }
    }
    return new FijiRowDataPageWritable(pageData);
  }

  /**
   * Nested class for a paged result of this FijiRowDataWritable.  Writes the initial
   * FijiRowDataWritable, but overlays the specified column in the Writable result.
   */
  public class FijiRowDataPageWritable implements Writable {
    private final Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> mPageData;

    /**
     * @param pageData map of columns to the data to substitute for those columns
     */
    public FijiRowDataPageWritable(
        Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> pageData) {
      mPageData = pageData;
    }

    /**
     * Returns whether this FijiRowDataPageWritable has any paged cells to be substituted.
     * @return whether this FijiRowDataPageWritable has any paged cells to be substituted.
     */
    public boolean isEmpty() {
      for (NavigableMap<Long, FijiCellWritable> values : mPageData.values()) {
        if (!values.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataOutput out) throws IOException {
      writeWithPages(out, mPageData);
    }

    /** {@inheritDoc} */
    @Override
    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException(
          "Pages should be read as instances of FijiRowDataWritable.");
    }
  }

  /**
   * Returns an unmodifiable map of column names to timeseries of FijiCell data.  Note that the
   * individual timeseries are mutable collections.
   *
   * @return map of FijiColumnName to timeseries of data.
   */
  public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> getData() {
    return Collections.unmodifiableMap(mWritableData);
  }

  /**
   * Converts a timeseries of FijiCell data into a Writable timeseries for serialization.
   * @param timeseries from FijiRowData.
   * @return a Writable timeseries suitable for serialization.
   */
  private NavigableMap<Long, FijiCellWritable> convertCellsToWritable(
      NavigableMap<Long, FijiCell<Object>> timeseries) {
    NavigableMap<Long, FijiCellWritable> writableTimeseries =
        Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, FijiCell<Object>> entry : timeseries.entrySet()) {
      Long timestamp = entry.getKey();
      FijiCellWritable fijiCellWritable = new FijiCellWritable(entry.getValue());
      writableTimeseries.put(timestamp, fijiCellWritable);
    }
    return writableTimeseries;
  }

  /**
   * Extracts the cells from a Writable timeseries.
   * @param writableTimeseries generated from {@link #convertCellsToWritable}
   * @return timeseries data without Writable wrappers.
   */
  private NavigableMap<Long, Object> extractCellsfromWritable(
      NavigableMap<Long, FijiCellWritable> writableTimeseries) {
    Preconditions.checkNotNull(writableTimeseries);
    NavigableMap<Long, Object> timeseries = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, FijiCellWritable> entry : writableTimeseries.entrySet()) {
      Long timestamp = entry.getKey();
      FijiCellWritable fijiCellWritable = entry.getValue();
      Object cellData = fijiCellWritable.getData();
      timeseries.put(timestamp, cellData);
    }
    return timeseries;
  }

  /** @return decoded cell data(initializing it if necessary). */
  private Map<FijiColumnName, NavigableMap<Long, Object>> getDecodedData() {
    if (mDecodedData == null) {
      Preconditions.checkNotNull(mWritableData);
      mDecodedData = Maps.newHashMap();
      for (FijiColumnName column : mWritableData.keySet()) {
        NavigableMap<Long, FijiCellWritable> writableTimeSeries = mWritableData.get(column);
        mDecodedData.put(column, extractCellsfromWritable(writableTimeSeries));
      }
    }
    return mDecodedData;
  }

  /**
   * Initializes the list of associated column family FijiPagers for this FijiRowData.
   *
   * @param fijiDataRequest the data request for this FijiRowData.
   * @return map of families to their associated qualifier pagers.
   */
  private Map<String, FijiPager> getFijiQualifierPagers(FijiDataRequest fijiDataRequest) {
    Map<String, FijiPager> fijiQualifierPagers = Maps.newHashMap();
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      if (column.isPagingEnabled() && !column.getColumnName().isFullyQualified()) {
        // Only include pagers for column families.
        try {
          LOG.info("Paging enabled for column family: {}", column.getColumnName());
          FijiPager fijiPager = mRowData.getPager(column.getFamily());
          fijiQualifierPagers.put(column.getFamily(), fijiPager);
        } catch (FijiColumnPagingNotEnabledException e) {
          LOG.warn("Paging not enabled for column family: {}", column.getColumnName());
        }
      }
    }
    return fijiQualifierPagers;
  }

  /**
   * Initializes the list of associated fully qualified cell FijiPagers for this FijiRowData.  Any
   * non fully qualified cell paging configuration will be ignored.
   *
   * @param fijiDataRequest the data request for this FijiRowData.
   * @param fijiRowData the fijiRowData to generate the pagers from.
   * @return map of FijiColumnNames to their associated cell pagers.
   */
  private static Map<FijiColumnName, FijiPager> getFijiCellPagers(FijiDataRequest fijiDataRequest,
                                                                  FijiRowData fijiRowData) {
    Map<FijiColumnName, FijiPager> fijiCellPagers = Maps.newHashMap();
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      if (column.isPagingEnabled() && column.getColumnName().isFullyQualified()) {
        // Only include pagers for fully qualified cells
        try {
          LOG.info("Paging enabled for column: {}", column.getColumnName());
          FijiPager fijiPager = fijiRowData.getPager(column.getFamily(), column.getQualifier());
          fijiCellPagers.put(column.getColumnName(), fijiPager);
        } catch (FijiColumnPagingNotEnabledException e) {
          LOG.warn("Paging not enabled for column: {}", column.getColumnName());
        }
      }
    }
    return fijiCellPagers;
  }

  /** @return The row key. */
  public EntityIdWritable getEntityId() {
    return mEntityId;
  }

  /**
   * Determines whether a particular column family has data in this row.
   *
   * @param family Column family to check for.
   * @return Whether the specified column family has data in this row.
   */
  public boolean containsColumn(String family) {
    for (FijiColumnName column : mWritableData.keySet()) {
      if (family.equals(column.getFamily())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines whether a particular column has data in this row.
   *
   * @param family Column family of the column to check for.
   * @param qualifier Column qualifier of the column to check for.
   * @return Whether the specified column has data in this row.
   */
  public boolean containsColumn(String family, String qualifier) {
    FijiColumnName column = new FijiColumnName(family, qualifier);
    return mWritableData.keySet().contains(column);
  }

  /**
   * Gets the set of column qualifiers that exist in a column family in this row.
   *
   * @param family Column family to get column qualifiers from.
   * @return Set of column qualifiers that exist in the <code>family</code> column family.
   */
  public NavigableSet<String> getQualifiers(String family) {
    NavigableSet<String> qualifiers = Sets.newTreeSet();
    for (FijiColumnName column : getDecodedData().keySet()) {
      if (family.equals(column.getFamily())) {
        qualifiers.add(column.getQualifier());
      }
    }
    return qualifiers;
  }

  /**
   * Gets the reader schema for a column as declared in the layout of the table this row
   * comes from.  Opportunistically checks the family as though it's a map type family if the
   * qualifier isn't found.
   *
   * @param family Column family of the desired column schema.
   * @param qualifier Column qualifier of the desired column schema.
   * @return Avro reader schema for the column.
   * @throws IOException If there is an error or the column does not exist.
   * @see com.moz.fiji.schema.layout.FijiTableLayout
   */
  public Schema getReaderSchema(String family, String qualifier) throws IOException {
    FijiColumnName column = new FijiColumnName(family, qualifier);
    if (mSchemas.containsKey(column)) {
      return mSchemas.get(column);
    } else {
      return mSchemas.get(new FijiColumnName(family));
    }
  }

  /**
   * Gets all data stored within the specified column family.
   *
   * @param family Map type column family of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column family.
   * @throws IOException If there is an error.
   */
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family)
      throws IOException {
    NavigableMap<String, NavigableMap<Long, T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      NavigableMap<Long, T> values = getValues(family, qualifier);
      result.put(qualifier, values);
    }
    return result;
  }

  /**
   * Gets all data stored within the specified column.
   *
   * @param family Column family of the desired data.
   * @param qualifier Column qualifier of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column.
   * @throws IOException If there is an error.
   */
  public <T> NavigableMap<Long, T> getValues(String family, String qualifier) throws IOException {
    FijiColumnName column = new FijiColumnName(family, qualifier);
    return (NavigableMap<Long, T>) getDecodedData().get(column);
  }

  /**
   * Helper method for the {@link org.apache.hadoop.io.Writable} interface that for writing
   * FijiRowDataWritable objects.  If passed a FijiColumnName, it will replace the data for the
   * specified column(relevant for paging through results).
   *
   * @param out DataOutput for the Hadoop Writable to write to.
   * @param pageData map of columns to paged data to be substituted(or an empty map if there are
   *                 no pages to substitute).
   * @throws IOException if there was an issue.
   */
  protected void writeWithPages(DataOutput out,
                                Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> pageData)
      throws IOException {

    // Write the EntityId
    mEntityId.write(out);

    // Count the total number of columns to write.
    Set<FijiColumnName> columnNames = Sets.newHashSet();
    for (FijiColumnName columnName : mWritableData.keySet()) {
      if (!mFijiQualifierPagers.containsKey(columnName.getFamily())) {
        columnNames.add(columnName);
      }
    }
    columnNames.addAll(pageData.keySet());
    WritableUtils.writeVInt(out, columnNames.size());

    // Write the unpaged data.
    for (Entry<FijiColumnName, NavigableMap<Long, FijiCellWritable>> entry
        : mWritableData.entrySet()) {
      FijiColumnName fijiColumnName = entry.getKey();
      if (!pageData.containsKey(fijiColumnName)
          && !mFijiQualifierPagers.containsKey(fijiColumnName.getFamily())) {
        // Only write if it's not part of the paged data.
        writeColumn(out, fijiColumnName, entry.getValue());
      }
    }

    // Write paged data if any.
    for (Entry<FijiColumnName, NavigableMap<Long, FijiCellWritable>> entry
        : pageData.entrySet()) {
      writeColumn(out, entry.getKey(), entry.getValue());
    }

    WritableUtils.writeVInt(out, mSchemas.size());
    for (Map.Entry<FijiColumnName, Schema> entry : mSchemas.entrySet()) {
      WritableUtils.writeString(out, entry.getKey().getName());
      WritableUtils.writeString(out, entry.getValue().toString());
    }
  }

  /**
   * Helper function to write a column and its associated data.
   *
   * @param out DataOutput for the Hadoop Writable to write to.
   * @param fijiColumnName to write
   * @param data to write
   * @throws IOException if there was an issue.
   */
  private void writeColumn(DataOutput out, FijiColumnName fijiColumnName,
                           NavigableMap<Long, FijiCellWritable> data) throws IOException {
    WritableUtils.writeString(out, fijiColumnName.getName());
    WritableUtils.writeVInt(out, data.size()); // number in the timeseries
    for (Map.Entry<Long, FijiCellWritable> cellEntry : data.entrySet()) {
      WritableUtils.writeVLong(out, cellEntry.getKey());
      cellEntry.getValue().write(out);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // By default write unsubstituted columns.
    writeWithPages(out, Collections.EMPTY_MAP);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    EntityIdWritable entityIdWritable =
        (EntityIdWritable) WritableFactories.newInstance(EntityIdWritable.class);
    entityIdWritable.readFields(in);
    mEntityId = entityIdWritable;

    int numDecodedData = WritableUtils.readVInt(in);

    // We need to dirty the decoded data so that these objects can be reused.
    mDecodedData = null;

    mWritableData = Maps.newHashMap();
    for (int c = 0; c < numDecodedData; c++) {
      String columnText = WritableUtils.readString(in);
      FijiColumnName column = new FijiColumnName(columnText);

      NavigableMap<Long, FijiCellWritable> data = Maps.newTreeMap();
      int numCells = WritableUtils.readVInt(in);
      for (int d = 0; d < numCells; d++) {
        long ts = WritableUtils.readVLong(in);
        FijiCellWritable cellWritable =
            (FijiCellWritable) WritableFactories.newInstance(FijiCellWritable.class);
        cellWritable.readFields(in);
        data.put(ts, cellWritable);
      }

      mWritableData.put(column, data);
    }

    mSchemas = Maps.newHashMap();
    int numSchemas = WritableUtils.readVInt(in);
    for (int c=0; c < numSchemas; c++) {
      String columnText = WritableUtils.readString(in);
      FijiColumnName column = new FijiColumnName(columnText);
      String schemaString = WritableUtils.readString(in);
      Schema schema = new Schema.Parser().parse(schemaString);
      mSchemas.put(column, schema);
    }
  }
}
