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

package com.moz.fiji.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.hive.io.FijiCellWritable;
import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.hive.utils.HiveTypes.HiveList;
import com.moz.fiji.hive.utils.HiveTypes.HiveMap;
import com.moz.fiji.hive.utils.HiveTypes.HiveStruct;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/**
 * Tests for parsing and evaluation of FijiRowExpressions.
 */
public class TestFijiRowExpression extends FijiClientTest {
  private EntityId mEntityId;
  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;

  // ObjectInspectors to be created once and used in the tests.qgqq
  private ObjectInspector mColumnFlatValueObjectInspector;
  private ObjectInspector mColumnAllValuesObjectInspector;
  private ObjectInspector mFamilyFlatValueObjectInspector;
  private ObjectInspector mFamilyAllValuesObjectInspector;

  @Before
  public final void setupFijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mFiji = getFiji();
    mFiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.ROW_DATA_TEST));
    mTable = mFiji.openTable("row_data_test_table");
    mEntityId = mTable.getEntityId("row1");
    final FijiTableWriter writer = mTable.openTableWriter();
    try {
      // Map type column family
      writer.put(mEntityId, "map", "qualA", 1L, 5);
      writer.put(mEntityId, "map", "qualA", 2L, 6);
      writer.put(mEntityId, "map", "qualA", 3L, 7);
      writer.put(mEntityId, "map", "qualB", 2L, 8);

      // Regular column family
      writer.put(mEntityId, "family", "qual0", 1L, "a");
      writer.put(mEntityId, "family", "qual0", 2L, "b");
      writer.put(mEntityId, "family", "qual0", 3L, "c");
      writer.put(mEntityId, "family", "qual1", 2L, "d");
    } finally {
      writer.close();
    }
    mReader = mTable.openTableReader();

    // Create a ObjectInspector for timeseries data that we expect to get from Hive.
    List<String> columnNames = Lists.newArrayList("timestamp", "value");
    List<ObjectInspector> objectInspectors = Lists.newArrayList();
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    // Setup relevant ObjectInspectors

    // STRUCT<TIMESTAMP, cell>
    mColumnFlatValueObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            columnNames,
            objectInspectors);

    // ARRAY<STRUCT<TIMESTAMP, cell>>
    mColumnAllValuesObjectInspector =
        ObjectInspectorFactory.getStandardListObjectInspector(
            mColumnFlatValueObjectInspector);

    // MAP<STRING, STRUCT<TIMESTAMP, cell>>
    mFamilyFlatValueObjectInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            mColumnFlatValueObjectInspector);

    // MAP<STRING, ARRAY<STRUCT<TIMESTAMP, cell>>>
    mFamilyAllValuesObjectInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            mColumnAllValuesObjectInspector);
  }

  @After
  public final void teardownFijiInstance() throws IOException {
    mReader.close();
    mTable.release();
    mFiji.deleteTable("row_data_test_table");
  }

  @Test
  public void testEntityIdExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(":entity_id", TypeInfos.ENTITY_ID);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(0, fijiDataRequest.getColumns().size());

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    String result = (String) fijiRowExpression.evaluate(fijiRowDataWritable);
    assertEquals(mEntityId.toShellString(), result);
  }

  @Test
  public void testFamilyAllValuesExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("map", TypeInfos.FAMILY_MAP_ALL_VALUES);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertFalse(column.getColumnName().isFullyQualified());
      assertEquals(HConstants.ALL_VERSIONS,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    HiveMap resultMap = (HiveMap) fijiRowExpression.evaluate(fijiRowDataWritable);
    HiveList qualAList = (HiveList) resultMap.get("qualA");
    assertEquals(3, qualAList.size());

    HiveList qualBList = (HiveList) resultMap.get("qualB");
    assertEquals(1, qualBList.size());
  }

  @Test
  public void testFamilyAllValuesExpressionWrites() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("map", TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<Object> hiveCellA1 = createHiveTimestampedCell(3L, "c");
    List<Object> hiveCellA2 = createHiveTimestampedCell(5L, "e");
    List<Object> hiveCellsA = Lists.newArrayList();
    hiveCellsA.add(hiveCellA1);
    hiveCellsA.add(hiveCellA2);
    List<Object> hiveCellB1 = createHiveTimestampedCell(4L, "d");
    List<Object> hiveCellB2 = createHiveTimestampedCell(6L, "f");
    List<Object> hiveCellsB = Lists.newArrayList();
    hiveCellsB.add(hiveCellB1);
    hiveCellsB.add(hiveCellB2);

    Map<String, Object> hiveData = Maps.newHashMap();
    hiveData.put("qualA", hiveCellsA);
    hiveData.put("qualB", hiveCellsB);

    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData =
        fijiRowExpression.convertToTimeSeries(mFamilyAllValuesObjectInspector, hiveData);

    FijiColumnName fijiColumnNameA = new FijiColumnName("map", "qualA");
    assertTrue(expressionData.containsKey(fijiColumnNameA));
    NavigableMap<Long, FijiCellWritable> timeseriesA = expressionData.get(fijiColumnNameA);
    validateCell(hiveCellA1, timeseriesA);
    validateCell(hiveCellA2, timeseriesA);

    FijiColumnName fijiColumnNameB = new FijiColumnName("map", "qualB");
    assertTrue(expressionData.containsKey(fijiColumnNameB));
    NavigableMap<Long, FijiCellWritable> timeseriesB = expressionData.get(fijiColumnNameB);
    validateCell(hiveCellB1, timeseriesB);
    validateCell(hiveCellB2, timeseriesB);

  }

  @Test
  public void testFamilyFlatValueExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("map[0]", TypeInfos.FAMILY_MAP_FLAT_VALUE);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertFalse(column.getColumnName().isFullyQualified());
      assertEquals(1,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    HiveMap resultMap = (HiveMap) fijiRowExpression.evaluate(fijiRowDataWritable);
    HiveStruct qualA = (HiveStruct) resultMap.get("qualA");
    assertEquals(new Date(3L), qualA.get(0));
    assertEquals(7, qualA.get(1));

    HiveStruct qualB = (HiveStruct) resultMap.get("qualB");
    assertEquals(new Date(2L), qualB.get(0));
    assertEquals(8, qualB.get(1));
  }

  @Test
  public void testFamilyFlatValueExpressionWrites() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("map[0]", TypeInfos.FAMILY_MAP_FLAT_VALUE);
    List<Object> hiveCellA = createHiveTimestampedCell(3L, "c");
    List<Object> hiveCellB = createHiveTimestampedCell(4L, "d");

    Map<String, Object> hiveData = Maps.newHashMap();
    hiveData.put("qualA", hiveCellA);
    hiveData.put("qualB", hiveCellB);

    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData =
        fijiRowExpression.convertToTimeSeries(mFamilyFlatValueObjectInspector, hiveData);

    FijiColumnName fijiColumnNameA = new FijiColumnName("map", "qualA");
    assertTrue(expressionData.containsKey(fijiColumnNameA));
    NavigableMap<Long, FijiCellWritable> timeseriesA = expressionData.get(fijiColumnNameA);
    validateCell(hiveCellA, timeseriesA);

    FijiColumnName fijiColumnNameB = new FijiColumnName("map", "qualB");
    assertTrue(expressionData.containsKey(fijiColumnNameB));
    NavigableMap<Long, FijiCellWritable> timeseriesB = expressionData.get(fijiColumnNameB);
    validateCell(hiveCellB, timeseriesB);
  }

  @Test
  public void testColumnAllValuesExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("family", "qual0"));
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(HConstants.ALL_VERSIONS,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    HiveList resultList = (HiveList) fijiRowExpression.evaluate(fijiRowDataWritable);
    assertEquals(3, resultList.size());
  }

  @Test
  public void testColumnAllValueExpressionWrites() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);
    List<Object> hiveCell1 = createHiveTimestampedCell(3L, "c");
    List<Object> hiveCell2 = createHiveTimestampedCell(4L, "d");

    List<Object> hiveData = Lists.newArrayList();
    hiveData.add(hiveCell1);
    hiveData.add(hiveCell2);

    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData =
        fijiRowExpression.convertToTimeSeries(mColumnAllValuesObjectInspector, hiveData);

    FijiColumnName fijiColumnName = new FijiColumnName("family", "qual0");
    assertTrue(expressionData.containsKey(fijiColumnName));
    NavigableMap<Long, FijiCellWritable> timeseries = expressionData.get(fijiColumnName);
    validateCell(hiveCell1, timeseries);
    validateCell(hiveCell2, timeseries);
  }

  @Test
  public void testColumnFlatValueExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0[0]", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("family", "qual0"));
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    HiveStruct resultStruct = (HiveStruct) fijiRowExpression.evaluate(fijiRowDataWritable);
    assertEquals(new Date(3L), resultStruct.get(0));
    assertEquals("c", resultStruct.get(1));
  }

  @Test
  public void testColumnFlatValueExpressionWrites() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0[0]", TypeInfos.COLUMN_FLAT_VALUE);

    List<Object> hiveCell = createHiveTimestampedCell(3L, "c");

    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData =
        fijiRowExpression.convertToTimeSeries(mColumnFlatValueObjectInspector, hiveCell);
    FijiColumnName fijiColumnName = new FijiColumnName("family", "qual0");
    assertTrue(expressionData.containsKey(fijiColumnName));
    NavigableMap<Long, FijiCellWritable> timeseries = expressionData.get(fijiColumnName);
    validateCell(hiveCell, timeseries);
  }

  @Test
  public void testColumnFlatValueFieldExpression() {
    // This expression returns the struct type, and then Hive extracts the relevant part
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0[0].value", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("family", "qual0"));
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier())
              .getMaxVersions());
    }
  }

  @Test
  public void testColumnFlatValueTimestampExpression() {
    // This expression returns the struct type, and then Hive extracts the relevant part
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0[0].ts", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("family", "qual0"));
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testDefaultVersions() {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);

    // Test that the FijiDataRequest was constructed correctly
    FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(
          HConstants.ALL_VERSIONS,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnSpecificFlatValueExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0[1]", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the FijiDataRequest was constructed correctly
    FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("family", "qual0"));
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertEquals(2,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    HiveStruct resultStruct = (HiveStruct) fijiRowExpression.evaluate(fijiRowDataWritable);
    assertEquals(new Date(2L), resultStruct.get(0));
    assertEquals("b", resultStruct.get(1));
  }

  @Test
  public void testColumnOldestFlatValueExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("family:qual0[-1]", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the FijiDataRequest was constructed correctly
    FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("family", "qual0"));
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertEquals(HConstants.ALL_VERSIONS,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    HiveStruct resultStruct = (HiveStruct) fijiRowExpression.evaluate(fijiRowDataWritable);
    assertEquals(new Date(1L), resultStruct.get(0));
    assertEquals("a", resultStruct.get(1));
  }

  @Test
  public void testInvalidIndex() {
    try {
      final FijiRowExpression fijiRowExpression =
          new FijiRowExpression("family:qual0[-2].timestamp", TypeInfos.COLUMN_FLAT_VALUE);
      fail("Should fail with a RuntimeException");
    } catch (RuntimeException re) {
      assertEquals("Invalid index(must be >= -1): -2", re.getMessage());
    }
  }

  /**
   * Helper method to create Hive timestampped cells from a timestamp and data object.
   *
   * @param timestamp of the Hive cell as a Long.
   * @param data Object representing the data
   * @return a List(which is a Hive struct) representing what Hive might pass into the Hive Adapter.
   */
  private static List<Object> createHiveTimestampedCell(Long timestamp, Object data) {
    List<Object> hiveTimestamppedCell = Lists.newArrayList();
    hiveTimestamppedCell.add(new Timestamp(timestamp));
    hiveTimestamppedCell.add(data);
    return hiveTimestamppedCell;
  }

  /**
   * Helper method to validate that a particular Hive cell is the Writable time series.
   *
   * @param expectedCell List containing a timeseries and data object.
   * @param actual timeseries of FijiCellWritables.
   */
  private static void validateCell(List<Object> expectedCell,
                                   NavigableMap<Long, FijiCellWritable> actual) {
    assertEquals(2, expectedCell.size());
    long timestamp = ((Timestamp) expectedCell.get(0)).getTime();
    Object data = expectedCell.get(1);

    assertTrue(actual.containsKey(timestamp));
    FijiCellWritable fijiCellWritable = actual.get(timestamp);
    assertEquals((long) timestamp, fijiCellWritable.getTimestamp());
    assertEquals(data, fijiCellWritable.getData());
  }
}
