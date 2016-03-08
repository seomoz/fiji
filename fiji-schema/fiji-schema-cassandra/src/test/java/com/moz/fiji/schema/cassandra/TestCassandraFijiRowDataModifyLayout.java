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

package com.moz.fiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestCassandraFijiRowDataModifyLayout {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCassandraFijiRowDataModifyLayout.class);

  /** Test layout. */
  public static final String TEST_LAYOUT_V1 =
      "com.moz.fiji/schema/layout/TestHBaseFijiRowData.test-layout-v1.json";

  /** Update for TEST_LAYOUT, with Test layout with column "family:qual0" removed. */
  public static final String TEST_LAYOUT_V2 =
      "com.moz.fiji/schema/layout/TestHBaseFijiRowData.test-layout-v2.json";

  /** Layout for table 'writer_schema' to test when a column class is not found. */
  public static final String WRITER_SCHEMA_TEST =
      "com.moz.fiji/schema/layout/TestHBaseFijiRowData.writer-schema.json";

  /** Test layout with version layout-1.3. */
  public static final String TEST_LAYOUT_V1_3 =
      "com.moz.fiji/schema/layout/TestHBaseFijiRowData.layout-v1.3.json";

  private static final String TABLE_NAME = "row_data_test_table";

  private static final CassandraFijiClientTest CLIENT_TEST_DELEGATE = new CassandraFijiClientTest();
  private static Fiji mFiji;

  // Create a shared Fiji for everyone.
  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupFijiTest();
    mFiji = CLIENT_TEST_DELEGATE.getFiji();
  }

  @AfterClass
  public static void tearDownShared() throws Exception {
    CLIENT_TEST_DELEGATE.tearDownFijiTest();
  }

  // Tests for FijiRowData.getReaderSchema() with layout-1.3 tables.
  @Test
  public void testGetReaderSchemaLayout13() throws Exception {
    final Fiji fiji = new InstanceBuilder(mFiji)
        .withTable(FijiTableLayouts.getLayout(TEST_LAYOUT_V1_3))
        .build();
    final FijiTable table = fiji.openTable("table");
    try {
      final FijiTableReader reader = table.getReaderFactory().openTableReader();
      try {
        final EntityId eid = table.getEntityId("row");
        final FijiDataRequest dataRequest = FijiDataRequest.builder()
            .addColumns(ColumnsDef.create().addFamily("family"))
            .build();
        final FijiRowData row = reader.get(eid, dataRequest);
        assertEquals(
            Schema.Type.STRING,
            row.getReaderSchema("family", "qual0").getType());

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  // Tests that reading an entire family with a column that has been deleted works.
  @Test
  public void testReadDeletedColumns() throws Exception {
    // Create a separate Fiji here to avoid stepping on the one used elsewhere.
    mFiji.createTable(FijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    FijiTable table = mFiji.openTable(TABLE_NAME);
    try {
      new InstanceBuilder(mFiji)
          .withTable(table)
          .withRow("row1")
          .withFamily("family")
          .withQualifier("qual0").withValue(1L, "string1")
          .withQualifier("qual0").withValue(2L, "string2")
          .build();

      final TableLayoutDesc update = FijiTableLayouts.getLayout(TEST_LAYOUT_V2);
      update.setReferenceLayout(table.getLayout().getDesc().getLayoutId());
      mFiji.modifyTableLayout(update);

      final FijiDataRequest dataRequest = FijiDataRequest.builder()
          .addColumns(ColumnsDef.create().addFamily("family"))
          .build();

      final FijiTableReader reader = table.openTableReader();
      try {
        final FijiRowData row1 = reader.get(table.getEntityId("row1"), dataRequest);
        assertTrue(row1.getValues("family", "qual0").isEmpty());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  //    Tests that we can read a record using the writer schema.
  //    This tests the case when a specific record class is not found on the classpath.
  //    However, this behavior is bogus. The reader schema should not be tied to the classes
  //    available on the classpath.
  //
  //    TODO(SCHEMA-295) the user may force using the writer schemas by overriding the
  //        declared reader schemas. This test will be updated accordingly.
  @Test
  public void testWSchemaWhenSpecRecClassNF() throws Exception {
    final Fiji fiji = mFiji;
    fiji.createTable(FijiTableLayouts.getLayout(WRITER_SCHEMA_TEST));
    final FijiTable table = fiji.openTable("writer_schema");
    try {
      // Write a (generic) record:
      final Schema writerSchema = Schema.createRecord("Found", null, "class.not", false);
      writerSchema.setFields(Lists.newArrayList(
          new Field("field", Schema.create(Schema.Type.STRING), null, null)));

      final FijiTableWriter writer = table.openTableWriter();
      try {
        final GenericData.Record record = new GenericRecordBuilder(writerSchema)
            .set("field", "value")
            .build();
        writer.put(table.getEntityId("eid"), "family", "qualifier", 1L, record);

      } finally {
        writer.close();
      }

      // Read the record back (should be a generic record):
      final FijiTableReader reader = table.openTableReader();
      try {
        final FijiDataRequest dataRequest = FijiDataRequest.builder()
            .addColumns(ColumnsDef.create().add("family", "qualifier"))
            .build();
        final FijiRowData row = reader.get(table.getEntityId("eid"), dataRequest);
        final GenericData.Record record = row.getValue("family", "qualifier", 1L);
        assertEquals(writerSchema, record.getSchema());
        assertEquals("value", record.get("field").toString());
      } finally {
        reader.close();
      }

    } finally {
      table.release();
    }
  }
}
