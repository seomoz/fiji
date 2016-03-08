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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.moz.fiji.schema.avro.KeyValueBackupEntry;
import com.moz.fiji.schema.avro.MetaTableBackup;
import com.moz.fiji.schema.avro.MetadataBackup;
import com.moz.fiji.schema.avro.SchemaTableBackup;
import com.moz.fiji.schema.avro.SchemaTableEntry;
import com.moz.fiji.schema.avro.SystemTableBackup;
import com.moz.fiji.schema.avro.SystemTableEntry;
import com.moz.fiji.schema.avro.TableBackup;
import com.moz.fiji.schema.avro.TableLayoutBackupEntry;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.impl.MetadataRestorer;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/** Tests backuping and restoring Fiji meta tables. */
public class TestFijiMetaTable extends FijiClientTest {

  private static final byte[] BYTES_VALUE = Bytes.toBytes("value");

  @Test
  public void testBackupAndRestore() throws InterruptedException, IOException {
    final Fiji fiji = getFiji();
    final FijiMetaTable metaTable = fiji.getMetaTable();
    final FijiSchemaTable schemaTable = fiji.getSchemaTable();
    final FijiSystemTable systemTable = fiji.getSystemTable();

    final TableLayoutDesc layout = FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
    final FijiTableLayout updatedLayout = metaTable.updateTableLayout("foo", layout);
    metaTable.putValue("foo", "key", BYTES_VALUE);

    systemTable.putValue("testKey", Bytes.toBytes("testValue"));
    assertEquals(1, metaTable.listTables().size());
    assertEquals(1, metaTable.tableSet().size());
    assertEquals(1, metaTable.keySet("foo").size());
    assertArrayEquals(BYTES_VALUE, metaTable.getValue("foo", "key"));
    // write to backupBuilder
    final MetadataBackup.Builder backupBuilder = MetadataBackup.newBuilder()
        .setLayoutVersion(fiji.getSystemTable().getDataVersion().toString())
        .setMetaTable(
            MetaTableBackup.newBuilder()
                .setTables(new HashMap<String, TableBackup>())
                .build())
        .setSchemaTable(
            SchemaTableBackup.newBuilder()
                .setEntries(new ArrayList<SchemaTableEntry>())
                .build())
        .setSystemTable(
            SystemTableBackup.newBuilder()
                .setEntries(new ArrayList<SystemTableEntry>())
                .build());
    backupBuilder.setMetaTable(metaTable.toBackup());
    backupBuilder.setSchemaTable(schemaTable.toBackup());
    backupBuilder.setSystemTable(systemTable.toBackup());
    final MetadataBackup backup = backupBuilder.build();

    // make sure metadata key-value pairs are what we expect.
    List<KeyValueBackupEntry> keyValues =
        backup.getMetaTable().getTables().get("foo").getKeyValueBackup().getKeyValues();
    assertEquals(1, keyValues.size());
    assertEquals("key", keyValues.get(0).getKey());
    assertArrayEquals(BYTES_VALUE, keyValues.get(0).getValue().array());

    // make sure layouts are what we expect.
    List<TableLayoutBackupEntry> layoutBackups =
        backup.getMetaTable().getTables().get("foo").getTableLayoutsBackup().getLayouts();
    assertEquals(1, layoutBackups.size());
    assertEquals(updatedLayout.getDesc(), layoutBackups.get(0).getLayout());

    metaTable.deleteTable("foo");
    assertTrue(!metaTable.tableSet().contains("foo"));
    assertEquals(0, metaTable.listTables().size());
    assertEquals(0, metaTable.tableSet().size());

    final MetadataRestorer restorer = new MetadataRestorer();
    restorer.restoreTables(backup, fiji);

    final FijiMetaTable newMetaTable = fiji.getMetaTable();
    assertEquals("The number of tables with layouts is incorrect.", 1,
        newMetaTable.listTables().size());
    assertEquals("The number of tables with kv pairs is incorrect.", 1,
        newMetaTable.tableSet().size());
    assertEquals("The number of keys for the foo table is incorrect.", 1,
        newMetaTable.keySet("foo").size());
    assertArrayEquals(BYTES_VALUE, newMetaTable.getValue("foo", "key"));

    systemTable.putValue("testKey", Bytes.toBytes("changedValue"));
    restorer.restoreSystemVars(backup, fiji);
    assertEquals("testValue", Bytes.toString(systemTable.getValue("testKey")));
  }

  @Test
  public void testSameMetaTableOnPut() throws InterruptedException, IOException {
    final Fiji fiji = getFiji();
    final FijiMetaTable metaTable = fiji.getMetaTable();

    final FijiTableKeyValueDatabase<?> outDb = metaTable.putValue("foo", "key", BYTES_VALUE);
    assertEquals("putValue() exposes the delegate", metaTable, outDb);
  }

  @Test
  public void testChainedMetaTable() throws InterruptedException, IOException {
    // Do an operation on the metatable, then set a key with putValue().
    // Use the FijiMetaTable obj returned by this to modify the underlying db.
    // Verify that the original FijiMetaTable sees the change.
    final Fiji fiji = getFiji();
    final FijiMetaTable metaTable = fiji.getMetaTable();

    final TableLayoutDesc layout = FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
    final FijiTableLayout updatedLayout = metaTable.updateTableLayout("foo", layout);

    final FijiMetaTable outMeta = metaTable.putValue("foo", "key", BYTES_VALUE);
    assertEquals("putValue() exposes the delegate", metaTable, outMeta);

    outMeta.deleteTable("foo");

    assertTrue(!outMeta.tableSet().contains("foo"));
    assertEquals(0, outMeta.listTables().size());
    assertEquals(0, outMeta.tableSet().size());

    assertTrue(!metaTable.tableSet().contains("foo"));
    assertEquals(0, metaTable.listTables().size());
    assertEquals(0, metaTable.tableSet().size());
  }

}
