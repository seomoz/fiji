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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestHiveTableDescription extends FijiClientTest {
  private static final Long TIMESTAMP = 1L;
  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout = FijiTableLayout.newLayout(
        FijiTableLayouts.getLayout(FijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mFiji = new InstanceBuilder()
        .withTable("user", layout)
          .withRow("foo")
            .withFamily("info")
        .withQualifier("name").withValue(TIMESTAMP, "foo-val")
        .build();

    // Fill local variables.
    mTable = mFiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    mReader.close();
    mTable.release();
    mFiji.release();
  }

  @Test
  public void testBuilder() throws IOException {
    List<String> columnNames = Lists.newArrayList();
    List<TypeInfo> columnTypes = Lists.newArrayList();
    List<String> columnExpressions = Lists.newArrayList();
    HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .build();
  }

  @Test
  public void testConstructDataRequestFromNoExpressions() throws IOException {
    List<String> columnNames = Lists.newArrayList();
    List<TypeInfo> columnTypes = Lists.newArrayList();
    List<String> columnExpressions = Lists.newArrayList();
    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .build();

    FijiDataRequest fijiDataRequest = hiveTableDescription.getDataRequest();
    assertTrue(fijiDataRequest.isEmpty());
  }

  @Test
  public void testConstructDataRequest() throws IOException {
    List<String> columnNames = Lists.newArrayList("info:name");
    List<TypeInfo> columnTypes = Lists.newArrayList();
    columnTypes.add(TypeInfos.COLUMN_ALL_VALUES);
    List<String> columnExpressions = Lists.newArrayList("info:name");

    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .build();

    final FijiDataRequest fijiDataRequest = hiveTableDescription.getDataRequest();
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("info", "name"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHBaseResultDecoding() throws IOException {
    List<String> columnNames = Lists.newArrayList("info:name");
    final TypeInfo typeInfo = TypeInfos.COLUMN_ALL_VALUES;
    List<TypeInfo> columnTypes = Lists.newArrayList(typeInfo);
    List<String> columnExpressions = Lists.newArrayList("info:name");

    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .build();

    final FijiDataRequest request = FijiDataRequest.create("info", "name");
    final FijiRowScanner scanner = mReader.getScanner(request);
    try {
      FijiRowData fijiRowData = scanner.iterator().next();
      FijiRowDataWritable result = new FijiRowDataWritable(fijiRowData, mReader);

      // array<>
      List<Object> decodedArray = (List) hiveTableDescription.createDataObject(result);
      assertEquals(1, decodedArray.size());

      // array<struct<>>
      List<Object> decodedStruct = (List) decodedArray.get(0);
      assertEquals(1, decodedStruct.size());

      // array<struct<ts:timestamp,value:string>>
      List<Object> decodedElement = (List) decodedStruct.get(0);
      assertEquals(2, decodedElement.size());

      Timestamp timestamp = (Timestamp) decodedElement.get(0);
      assertTrue(timestamp.equals(new Timestamp(TIMESTAMP)));

      String name = (String) decodedElement.get(1);
      assertEquals("foo-val", name);
    } finally {
      scanner.close();
    }
  }
}
