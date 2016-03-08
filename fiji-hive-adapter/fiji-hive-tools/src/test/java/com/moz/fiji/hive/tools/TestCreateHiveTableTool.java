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

package com.moz.fiji.hive.tools;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestCreateHiveTableTool extends FijiClientTest {

  private static final FijiColumnName BOOLEAN_COLUMN =
      new FijiColumnName("primitive:boolean_column");
  private static final FijiColumnName INT_COLUMN = new FijiColumnName("primitive:int_column");
  private static final FijiColumnName LONG_COLUMN = new FijiColumnName("primitive:long_column");
  private static final FijiColumnName FLOAT_COLUMN = new FijiColumnName("primitive:float_column");
  private static final FijiColumnName DOUBLE_COLUMN = new FijiColumnName("primitive:double_column");
  private static final FijiColumnName BYTES_COLUMN = new FijiColumnName("primitive:bytes_column");
  private static final FijiColumnName STRING_COLUMN = new FijiColumnName("primitive:string_column");

  private static final FijiColumnName RECORD_COLUMN = new FijiColumnName("complex:record_column");
  private static final FijiColumnName ENUM_COLUMN = new FijiColumnName("complex:enum_column");
  private static final FijiColumnName ARRAY_COLUMN = new FijiColumnName("complex:array_column");
  private static final FijiColumnName MAP_COLUMN = new FijiColumnName("complex:map_column");
  private static final FijiColumnName UNION_COLUMN = new FijiColumnName("complex:union_column");
  private static final FijiColumnName FIXED_COLUMN = new FijiColumnName("complex:fixed_column");
  private static final FijiColumnName RECURSIVE_RECORD_COLUMN =
      new FijiColumnName("complex:recursive_record_column");
  private static final FijiColumnName CLASS_COLUMN = new FijiColumnName("complex:class_column");

  private static final FijiColumnName BOOLEAN_MAP_FAMILY = new FijiColumnName("boolean_map");
  private static final FijiColumnName INT_MAP_FAMILY = new FijiColumnName("int_map");
  private static final FijiColumnName LONG_MAP_FAMILY = new FijiColumnName("long_map");
  private static final FijiColumnName FLOAT_MAP_FAMILY = new FijiColumnName("float_map");
  private static final FijiColumnName DOUBLE_MAP_FAMILY = new FijiColumnName("double_map");
  private static final FijiColumnName BYTES_MAP_FAMILY = new FijiColumnName("bytes_map");
  private static final FijiColumnName STRING_MAP_FAMILY = new FijiColumnName("string_map");

  private Fiji mFiji;
  private FijiSchemaTable mSchemaTable;

  // Allow the constructor of this class to throw an exception to load the mLayout from the file
  public TestCreateHiveTableTool() throws IOException {}

  private final FijiTableLayout mLayout = FijiTableLayout.newLayout(
      FijiTableLayouts.getLayout("com.moz.fiji/schema/layout/all-types-schema.json"));

  @Before
  public final void setupFijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mFiji = getFiji();
    mFiji.createTable(mLayout.getDesc());
    mSchemaTable = mFiji.getSchemaTable();
  }

  @After
  public final void teardownFijiInstance() throws IOException {
    mFiji.deleteTable("all_types_table");
  }

  @Test
  public void testPrimitivesSchemas() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BOOLEAN>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BOOLEAN_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: INT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(INT_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BIGINT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(LONG_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: FLOAT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(FLOAT_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: DOUBLE>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(DOUBLE_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BYTES_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(STRING_COLUMN)));
  }

  @Test
  public void testPrimitiveLayouts() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BOOLEAN>",
        CreateHiveTableTool.getHiveType(BOOLEAN_COLUMN, mLayout, mSchemaTable));
    assertEquals("STRUCT<ts: TIMESTAMP, value: INT>",
        CreateHiveTableTool.getHiveType(INT_COLUMN, mLayout, mSchemaTable));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BIGINT>",
        CreateHiveTableTool.getHiveType(LONG_COLUMN, mLayout, mSchemaTable));
    assertEquals("STRUCT<ts: TIMESTAMP, value: FLOAT>",
        CreateHiveTableTool.getHiveType(FLOAT_COLUMN, mLayout, mSchemaTable));
    assertEquals("STRUCT<ts: TIMESTAMP, value: DOUBLE>",
        CreateHiveTableTool.getHiveType(DOUBLE_COLUMN, mLayout, mSchemaTable));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.getHiveType(BYTES_COLUMN, mLayout, mSchemaTable));
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.getHiveType(STRING_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testPrimitiveMapSchemas() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BOOLEAN>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BOOLEAN_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: INT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(INT_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BIGINT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(LONG_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: FLOAT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(FLOAT_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: DOUBLE>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(DOUBLE_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BYTES_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(STRING_MAP_FAMILY)));
  }

  @Test
  public void testPrimitiveMaps() throws IOException {
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: BOOLEAN>>",
        CreateHiveTableTool.getHiveType(BOOLEAN_MAP_FAMILY, mLayout, mSchemaTable));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: INT>>",
        CreateHiveTableTool.getHiveType(INT_MAP_FAMILY, mLayout, mSchemaTable));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: BIGINT>>",
        CreateHiveTableTool.getHiveType(LONG_MAP_FAMILY, mLayout, mSchemaTable));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: FLOAT>>",
        CreateHiveTableTool.getHiveType(FLOAT_MAP_FAMILY, mLayout, mSchemaTable));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: DOUBLE>>",
        CreateHiveTableTool.getHiveType(DOUBLE_MAP_FAMILY, mLayout, mSchemaTable));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: BINARY>>",
        CreateHiveTableTool.getHiveType(BYTES_MAP_FAMILY, mLayout, mSchemaTable));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: STRING>>",
        CreateHiveTableTool.getHiveType(STRING_MAP_FAMILY, mLayout, mSchemaTable));
  }

  @Test
  public void testRecord() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRUCT<`numerator`: INT, `denominator`: INT>>",
        CreateHiveTableTool.getHiveType(RECORD_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testRecursiveRecord() throws IOException {
    assertEquals(
      "STRUCT<ts: TIMESTAMP, value: STRUCT<`value`: BIGINT, `next`: "
      + "STRUCT<`value`: BIGINT, `next`: STRING>>>",
      CreateHiveTableTool.getHiveType(RECURSIVE_RECORD_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testEnum() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.getHiveType(ENUM_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testArray() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: ARRAY<STRING>>",
        CreateHiveTableTool.getHiveType(ARRAY_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testMap() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: MAP<STRING, BIGINT>>",
        CreateHiveTableTool.getHiveType(MAP_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testUnion() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.getHiveType(UNION_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testFixed() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.getHiveType(FIXED_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testClassColumn() throws IOException {
    String nodeClassHiveType =
        "STRUCT<ts: TIMESTAMP, value: STRUCT<`label`: STRING, `weight`: DOUBLE, "
          + "`edges`: ARRAY<STRUCT<`label`: STRING, `weight`: DOUBLE, " // begin edges1
            + "`target`: STRUCT<`label`: STRING, `weight`: DOUBLE, " // begin target1
              + "`edges`: ARRAY<STRUCT<`label`: STRING, `weight`: DOUBLE, " // begin edges2
                + "`target`: STRING, " // begin target2
                + "`annotations`: MAP<STRING, STRING>>>, " // end target2
              + "`annotations`: MAP<STRING, STRING>>, " // end edges2
            + "`annotations`: MAP<STRING, STRING>>>, " //end target1
          + "`annotations`: MAP<STRING, STRING>>>"; //end edges1
    assertEquals(nodeClassHiveType,
        CreateHiveTableTool.getHiveType(CLASS_COLUMN, mLayout, mSchemaTable));
  }

  @Test
  public void testGetDefaultHiveColumnName() {
    // Map type column family.
    assertEquals("family",
        CreateHiveTableTool.getDefaultHiveColumnName(new FijiColumnName("family")));
    // Fully qualified column.
    assertEquals("qualifier",
        CreateHiveTableTool.getDefaultHiveColumnName(new FijiColumnName("family", "qualifier")));
  }
}
