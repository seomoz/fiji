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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.hive.utils.FijiDataRequestSerializer;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestFijiTableRecordReader extends FijiClientTest {
  private static final Long TIMESTAMP = 1L;
  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;
  private Configuration mConf;

  private static final String TABLE_NAME = "user";

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout = FijiTableLayout.newLayout(
        FijiTableLayouts.getLayout(FijiTableLayouts.PAGING_TEST));

    // Populate the environment.
    mFiji = new InstanceBuilder()
        .withTable(TABLE_NAME, layout)
        .withRow("foo")
          .withFamily("info")
            .withQualifier("name").withValue(TIMESTAMP, "foo-val")
        .withRow("bar")
          .withFamily("info")
            .withQualifier("name").withValue(TIMESTAMP, "bar-val")
        .build();

    // Fill local variables.
    mTable = mFiji.openTable(TABLE_NAME);
    mReader = mTable.openTableReader();
    mConf = getConf();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    mReader.close();
    mTable.release();
    mFiji.release();
  }

  @Test
  public void testFetchData() throws IOException {
    FijiURI fijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    FijiTableInputSplit tableInputSplit =
        new FijiTableInputSplit(fijiURI, startKey, endKey, null, null);

    FijiDataRequest fijiDataRequest = FijiDataRequest.create("info", "name");
    mConf.set(FijiTableSerDe.HIVE_TABLE_NAME_PROPERTY, TABLE_NAME);
    mConf.set(FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX + TABLE_NAME,
        FijiDataRequestSerializer.serialize(fijiDataRequest));

    FijiTableRecordReader tableRecordReader = new FijiTableRecordReader(tableInputSplit, mConf);
    try {
      // Retrieve result
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      FijiRowDataWritable value = new FijiRowDataWritable();
      int resultCount = 0;
      boolean hasResult = tableRecordReader.next(key, value);
      while (hasResult) {
        resultCount++;
        hasResult = tableRecordReader.next(key, value);
      }
      assertEquals(2, resultCount);
    } finally {
      tableRecordReader.close();
    }
  }

  @Test
  public void testFetchPagedCellData() throws IOException {
    // Add some extra versions of the rows so that we can page through the results.
    FijiTableWriter fijiTableWriter = mTable.openTableWriter();
    try {
      EntityId entityId = mTable.getEntityId("foo");
      fijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 1, "foo-val-update1");
      fijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 2, "foo-val-update2");
    } finally {
      fijiTableWriter.close();
    }

    FijiURI fijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    FijiTableInputSplit tableInputSplit =
        new FijiTableInputSplit(fijiURI, startKey, endKey, null, null);

    FijiDataRequest fijiDataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).withPageSize(1).add("info", "name"))
        .build();

    mConf.set(FijiTableSerDe.HIVE_TABLE_NAME_PROPERTY, TABLE_NAME);
    mConf.set(FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX + TABLE_NAME,
        FijiDataRequestSerializer.serialize(fijiDataRequest));

    FijiTableRecordReader tableRecordReader = new FijiTableRecordReader(tableInputSplit, mConf);
    try {
      // Retrieve result
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      FijiRowDataWritable value = new FijiRowDataWritable();
      int resultCount = 0;
      boolean hasResult = tableRecordReader.next(key, value);
      while (hasResult) {
        resultCount++;
        hasResult = tableRecordReader.next(key, value);
      }

      // Should read 4 cells, 3 for foo, 1 for bar.  See testFetchData() for a nonpaged example that
      // has 2 results.
      assertEquals(4, resultCount);
    } finally {
      tableRecordReader.close();
    }
  }

  @Test
  public void testMultiplePagedCellColumns() throws IOException {
    // Add some extra versions of the rows so that we can page through the results.
    FijiTableWriter fijiTableWriter = mTable.openTableWriter();
    try {
      EntityId entityId = mTable.getEntityId("foo");
      fijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 1, "foo-val-update1");
      fijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 2, "foo-val-update2");
      fijiTableWriter.put(entityId, "info", "location", TIMESTAMP, "foo-location");
      fijiTableWriter.put(entityId, "info", "location", TIMESTAMP + 1, "foo-location-update1");
      fijiTableWriter.put(entityId, "info", "location", TIMESTAMP + 2, "foo-location-update2");
      fijiTableWriter.put(entityId, "info", "location", TIMESTAMP + 3, "foo-location-update3");
    } finally {
      fijiTableWriter.close();
    }

    FijiURI fijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    FijiTableInputSplit tableInputSplit =
        new FijiTableInputSplit(fijiURI, startKey, endKey, null, null);

    FijiDataRequest fijiDataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(9).withPageSize(1).add("info", "name"))
        .addColumns(ColumnsDef.create().withMaxVersions(9).withPageSize(1).add("info", "location"))
        .build();

    mConf.set(FijiTableSerDe.HIVE_TABLE_NAME_PROPERTY, TABLE_NAME);
    mConf.set(FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX + TABLE_NAME,
        FijiDataRequestSerializer.serialize(fijiDataRequest));

    FijiTableRecordReader tableRecordReader = new FijiTableRecordReader(tableInputSplit, mConf);
    try {
      // Retrieve result
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      FijiRowDataWritable value = new FijiRowDataWritable();
      int resultCount = 0;
      boolean hasResult = tableRecordReader.next(key, value);
      while (hasResult) {
        resultCount++;
        hasResult = tableRecordReader.next(key, value);
      }

      // Should read 4 cells, 3 for foo, 1 for bar.
      assertEquals(5, resultCount);
    } finally {
      tableRecordReader.close();
    }
  }

  @Test
  public void testFetchPagedQualifierData() throws IOException {
    // Add some extra versions of the rows so that we can page through the results.
    FijiTableWriter fijiTableWriter = mTable.openTableWriter();
    try {
      EntityId entityId = mTable.getEntityId("foo");
      fijiTableWriter.put(entityId, "jobs", "foo1", TIMESTAMP, "bar1");
      fijiTableWriter.put(entityId, "jobs", "foo2", TIMESTAMP, "bar2");
      fijiTableWriter.put(entityId, "jobs", "foo3", TIMESTAMP, "bar3");
      fijiTableWriter.put(entityId, "jobs", "foo4", TIMESTAMP, "bar4");
      fijiTableWriter.put(entityId, "jobs", "foo5", TIMESTAMP, "bar5");

      // To test KIJIHIVE-54. Let's add another row of data that has no "jobs" data
      // in order to test that we aren't trying to request paged data from a non-existent column
      // in that row.
      entityId = mTable.getEntityId("zbar");
      fijiTableWriter.put(entityId, "info", "name", TIMESTAMP + 2, "foo-val-update2");
    } finally {
      fijiTableWriter.close();
    }

    FijiURI fijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    FijiTableInputSplit tableInputSplit =
        new FijiTableInputSplit(fijiURI, startKey, endKey, null, null);

    FijiDataRequest fijiDataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withPageSize(2).addFamily("jobs"))
        .addColumns(ColumnsDef.create().addFamily("info"))
        .build();

    mConf.set(FijiTableSerDe.HIVE_TABLE_NAME_PROPERTY, TABLE_NAME);
    mConf.set(FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX + TABLE_NAME,
        FijiDataRequestSerializer.serialize(fijiDataRequest));

    // Initialize FijiTableRecordReader
    FijiTableRecordReader tableRecordReader = new FijiTableRecordReader(tableInputSplit, mConf);
    try {
      // Retrieve result
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      FijiRowDataWritable value = new FijiRowDataWritable();
      int resultCount = 0;
      boolean hasResult = tableRecordReader.next(key, value);

      while (hasResult) {
        resultCount++;
        hasResult = tableRecordReader.next(key, value);
        // Ensure that each page of mapped qualifier results is at most 3 (two for the jobs
        // and one for the info request).
        assertTrue(value.getData().size() <= 3);
      }

      // Should read 5 rows since we are requesting info and jobs columns from Fiji.
      assertEquals(5, resultCount);
    } finally {
      tableRecordReader.close();
    }
  }

  @Test
  public void testFetchPagedQualifierAndCellsData() throws IOException {
    // Add some extra versions of the rows so that we can page through the results.
    FijiTableWriter fijiTableWriter = mTable.openTableWriter();
    try {
      EntityId entityId = mTable.getEntityId("foo");
      fijiTableWriter.put(entityId, "jobs", "foo1", TIMESTAMP, "bar1");
      fijiTableWriter.put(entityId, "jobs", "foo1", TIMESTAMP + 1, "bar1+1");
      fijiTableWriter.put(entityId, "jobs", "foo1", TIMESTAMP + 2, "bar1+2");
      fijiTableWriter.put(entityId, "jobs", "foo2", TIMESTAMP, "bar2");
      fijiTableWriter.put(entityId, "jobs", "foo3", TIMESTAMP, "bar3");
    } finally {
      fijiTableWriter.close();
    }

    FijiURI fijiURI = mTable.getURI();
    byte[] startKey = new byte[0];
    byte[] endKey = new byte[0];
    FijiTableInputSplit tableInputSplit =
        new FijiTableInputSplit(fijiURI, startKey, endKey, null, null);

    FijiDataRequest fijiDataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(1)
            .withMaxVersions(HConstants.ALL_VERSIONS)
            .addFamily("jobs"))
        .build();

    mConf.set(FijiTableSerDe.HIVE_TABLE_NAME_PROPERTY, TABLE_NAME);
    mConf.set(FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX + TABLE_NAME,
        FijiDataRequestSerializer.serialize(fijiDataRequest));

    FijiTableRecordReader tableRecordReader = new FijiTableRecordReader(tableInputSplit, mConf);
    try {
      // Retrieve result
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      FijiRowDataWritable value = new FijiRowDataWritable();
      int resultCount = 0;
      boolean hasResult = tableRecordReader.next(key, value);
      while (hasResult) {
        resultCount++;
        hasResult = tableRecordReader.next(key, value);
        // Ensure that each page of mapped qualifier results is at most 2.
        assertTrue(value.getData().size() <= 2);
      }

      // Should read 3 rows
      assertEquals(5, resultCount);
    } finally {
      tableRecordReader.close();
    }
  }
}
