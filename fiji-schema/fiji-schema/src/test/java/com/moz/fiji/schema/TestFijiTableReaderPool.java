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
package com.moz.fiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.FijiTableReaderPool.Builder.WhenExhaustedAction;
import com.moz.fiji.schema.avro.EmptyRecord;
import com.moz.fiji.schema.avro.TestRecord1;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestFijiTableReaderPool extends FijiClientTest {

  private static final String FOO_NAME = "foo-name";
  private static final String FOO_EMAIL = "foo@email.com";
  private static final FijiDataRequest INFO_NAME = FijiDataRequest.create("info", "name");

  private FijiTable mTable;
  private EntityId mEID;

  @Before
  public void setup() throws IOException {
    new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.USER_TABLE))
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name")
                        .withValue(1, FOO_NAME)
                    .withQualifier("email")
                        .withValue(1, FOO_EMAIL)
        .build();
    mTable = getFiji().openTable("user");
    mEID = mTable.getEntityId("foo");
  }

  @After
  public void cleanup() throws IOException {
    mTable.release();
  }

  @Test
  public void testSimpleRead() throws Exception {
    final FijiTableReaderPool pool = FijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .build();
    try {
      final FijiTableReader reader = pool.borrowObject();
      try {
        assertEquals(
            FOO_NAME, reader.get(mEID, INFO_NAME).getMostRecentValue("info", "name").toString());
      } finally {
        reader.close();
      }
    } finally {
      pool.close();
    }
  }

  @Test
  public void testFullPool() throws Exception {
    final FijiTableReaderPool pool = FijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .withMaxActive(1)
        .withExhaustedAction(WhenExhaustedAction.FAIL)
        .build();
    try {
      final FijiTableReader reader = pool.borrowObject();
      try {
        final FijiTableReader reader2 = pool.borrowObject();
        fail("getReader() should have thrown NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        assertTrue(nsee.getMessage().equals("Pool exhausted"));
      } finally {
        reader.close();
      }
      // Because the first reader was returned to the pool, this one should work.
      final FijiTableReader reader3 = pool.borrowObject();
      reader3.close();
    } finally {
      pool.close();
    }
  }

  @Test
  public void testColumnReaderSpecOptions() throws Exception {
    new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.READER_SCHEMA_TEST))
            .withRow("row")
                .withFamily("family")
                    .withQualifier("empty")
                        .withValue(5, EmptyRecord.newBuilder().build())
        .build();
    final FijiTable table = getFiji().openTable("table");
    try {
      final FijiTableReaderPool pool = FijiTableReaderPool.Builder.create()
          .withReaderFactory(table.getReaderFactory())
          .withColumnReaderSpecOverrides(ImmutableMap.of(
              FijiColumnName.create("family", "empty"),
              ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class))
          ).build();
      try {
        final FijiTableReader reader = pool.borrowObject();
        try {
          final FijiDataRequest request = FijiDataRequest.create("family", "empty");
          final TestRecord1 record1 =
              reader.get(table.getEntityId("row"), request).getMostRecentValue("family", "empty");
          assertEquals(Integer.valueOf(-1), record1.getInteger());
        } finally {
          reader.close();
        }
      } finally {
        pool.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testRetainsTable() throws Exception {
    final FijiTable table = getFiji().openTable("user");
    final FijiTableReaderPool pool = FijiTableReaderPool.Builder.create()
        .withReaderFactory(table.getReaderFactory())
        .build();
    table.release();
    try {
      final FijiTableReader reader = pool.borrowObject();
      try {
        assertEquals(
            FOO_NAME, reader.get(mEID, INFO_NAME).getMostRecentValue("info", "name").toString());
      } finally {
        reader.close();
      }
    } finally {
      pool.close();
    }
  }
}
