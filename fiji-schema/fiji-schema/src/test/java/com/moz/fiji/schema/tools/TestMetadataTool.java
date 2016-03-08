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

package com.moz.fiji.schema.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiMetaTable;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestMetadataTool extends FijiToolTest {

  private static final String LAYOUT_V1 = "com.moz.fiji/schema/layout/layout-updater-v1.json";
  private static final String LAYOUT_V2 = "com.moz.fiji/schema/layout/layout-updater-v2.json";

  @Test
  public void testInTempFolder() throws Exception {
    final File backupFile = new File(getLocalTempDir(), "tempMetadataBackup");

    final Fiji fijiBackup = createTestFiji();
    final Fiji fijiRestored = createTestFiji();
    assertFalse(fijiBackup.getURI().equals(fijiRestored.getURI()));

    // Make this a non-trivial instance by creating a couple of tables.
    fijiBackup.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST));
    fijiBackup.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FULL_FEATURED));

    // Backup metadata for fijiBackup.
    List<String> args = Lists.newArrayList("--fiji=" + fijiBackup.getURI().toOrderedString(),
        "--backup=" + backupFile.getPath());
    MetadataTool tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));

    // Check that fijiBackup and fijiRestored do not intersect.
    assertFalse(fijiRestored.getTableNames().containsAll(fijiBackup.getTableNames()));

    // Restore metadata from fijiBackup to fijiRestored.
    args = Lists.newArrayList("--fiji=" + fijiRestored.getURI().toOrderedString(),
        "--restore=" + backupFile.getPath(),
        "--interactive=false");
    tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));
    assertTrue(fijiRestored.getTableNames().containsAll(fijiBackup.getTableNames()));
  }

  @Test
  public void testBackupUpdatedLayout() throws Exception {
    final File backupFile = new File(getLocalTempDir(), "tempMetadataBackup");

    final Fiji fijiBackup = createTestFiji();
    final Fiji fijiRestored = createTestFiji();
    assertFalse(fijiBackup.getURI().equals(fijiRestored.getURI()));

    // Create a table and update it's layout
    fijiBackup.createTable(FijiTableLayouts.getLayout(LAYOUT_V1));
    fijiBackup.modifyTableLayout(FijiTableLayouts.getLayout(LAYOUT_V2));
    FijiMetaTable backupMetaTable = fijiBackup.getMetaTable();
    assertEquals(2,
        backupMetaTable.getTableLayoutVersions("table_name", HConstants.ALL_VERSIONS).size());

    // Check that fijiBackup and fijiRestored do not intersect.
    assertFalse(fijiRestored.getTableNames().containsAll(fijiBackup.getTableNames()));

    // Backup metadata for fijiBackup.
    List<String> args = Lists.newArrayList("--fiji=" + fijiBackup.getURI().toOrderedString(),
        "--backup=" + backupFile.getPath());
    MetadataTool tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));

    // Restore metadata from fijiBackup to fijiRestored.
    args = Lists.newArrayList("--fiji=" + fijiRestored.getURI().toOrderedString(),
        "--restore=" + backupFile.getPath(),
        "--interactive=false");
    tool = new MetadataTool();
    tool.setConf(getConf());
    assertEquals(BaseTool.SUCCESS, tool.toolMain(args));

    // Validate that all tables are present with the correct number of versions
    assertTrue(fijiRestored.getTableNames().containsAll(fijiBackup.getTableNames()));
    FijiMetaTable restoreMetaTable = fijiRestored.getMetaTable();
    assertTrue(
        restoreMetaTable.getTableLayoutVersions("table_name", HConstants.ALL_VERSIONS).size() > 1);
  }
}
