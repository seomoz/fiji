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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;
import com.moz.fiji.schema.util.ResourceUtils;

/** Exercises FijiTableWriter delete functionality. */
public class IntegrationTestFijiTableWriterDeletes extends AbstractFijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestFijiTableWriterDeletes.class);

  private FijiDataRequest mDataRequest = null;
  private Fiji mFiji = null;
  private FijiTable mTable = null;
  private FijiTableWriter mWriter = null;
  private FijiTableReader mReader = null;

  /**
   * Creates a test table for deleting data.
   */
  @Before
  public final void setupIntegrationTestFijiTableWriterDeletes() throws Exception {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE)
        .addFamily("group")
        .addFamily("map")
        .addFamily("memoryMap");
    mDataRequest = builder.build();

    mFiji = Fiji.Factory.open(getFijiURI(), getConf());

    LOG.info("Creating test table.");
    final TableLayoutDesc layout = FijiTableLayouts.getLayout(FijiTableLayouts.DELETES_TEST);
    mFiji.createTable(layout);

    LOG.info("Populating test table.");
    mTable = mFiji.openTable(layout.getName());
    mWriter = mTable.openTableWriter();
    mWriter.put(mTable.getEntityId("alpha"), "group", "a", 1L, "1");
    mWriter.put(mTable.getEntityId("alpha"), "group", "a", 2L, "2");
    mWriter.put(mTable.getEntityId("alpha"), "group", "a", 3L, "3");
    mWriter.put(mTable.getEntityId("alpha"), "group", "b", 3L, "3");
    mWriter.put(mTable.getEntityId("alpha"), "map", "key1", 1L, "1");
    mWriter.put(mTable.getEntityId("alpha"), "map", "key2", 3L, "3");
    mWriter.put(mTable.getEntityId("alpha"), "memoryMap", "key1", 2L, "2");
    mWriter.put(mTable.getEntityId("alpha"), "memoryMap", "key2", 4L, "4");

    mReader = mTable.openTableReader();
    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
    LOG.info("Test table populated");
  }

  /**
   * Deletes the test table.
   */
  @After
  public final void teardownIntegrationTestFijiTableWriterDeletes() throws Exception {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.closeOrLog(mWriter);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mFiji);
  }

  @Test
  public void testDeleteRow() throws IOException {
    mWriter.deleteRow(mTable.getEntityId("alpha"));

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertFalse(result.containsColumn("group"));
    assertFalse(result.containsColumn("map"));
    assertFalse(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteRowUpToTimestamp() throws IOException {
    mWriter.deleteRow(mTable.getEntityId("alpha"), 3L);

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertFalse(result.containsColumn("group"));
    assertFalse(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
    assertFalse(result.getQualifiers("memoryMap").contains("key1"));
    assertTrue(result.getQualifiers("memoryMap").contains("key2"));
  }

  @Test
  public void testDeleteGroupFamily() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "group");

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertFalse(result.containsColumn("group"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteGroupFamilyUpToTimestamp() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "group", 2L);

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertEquals(1, result.getTimestamps("group", "a").size());
    assertTrue(result.getTimestamps("group", "a").contains(3L));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteMapFamily() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "map");

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertFalse(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteMapFamilyUpToTimestamp() throws IOException {
    mWriter.deleteFamily(mTable.getEntityId("alpha"), "map", 2L);

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.containsColumn("map"));
    assertEquals(1, result.getQualifiers("map").size());
    assertTrue(result.containsColumn("map", "key2"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteColumn() throws IOException {
    mWriter.deleteColumn(mTable.getEntityId("alpha"), "group", "a");

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertFalse(result.getQualifiers("group").contains("a"));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteColumnUpToTimestamp() throws IOException {
    mWriter.deleteColumn(mTable.getEntityId("alpha"), "group", "a", 2L);

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertEquals(1, result.getTimestamps("group", "a").size());
    assertTrue(result.getTimestamps("group", "a").contains(3L));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteCellMostRecent() throws IOException {
    mWriter.deleteCell(mTable.getEntityId("alpha"), "group", "a");

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.getQualifiers("group").contains("a"));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertFalse(result.getTimestamps("group", "a").contains(3L));
    assertTrue(result.getTimestamps("group", "a").contains(2L));
    assertTrue(result.getTimestamps("group", "a").contains(1L));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }

  @Test
  public void testDeleteCellAtTimestamp() throws IOException {
    mWriter.deleteCell(mTable.getEntityId("alpha"), "group", "a", 2L);

    FijiRowData result = mReader.get(mTable.getEntityId("alpha"), mDataRequest);
    assertTrue(result.containsColumn("group"));
    assertTrue(result.getQualifiers("group").contains("a"));
    assertTrue(result.getQualifiers("group").contains("b"));
    assertTrue(result.getTimestamps("group", "a").contains(3L));
    assertFalse(result.getTimestamps("group", "a").contains(2L));
    assertTrue(result.getTimestamps("group", "a").contains(1L));
    assertTrue(result.containsColumn("map"));
    assertTrue(result.containsColumn("memoryMap"));
  }
}
