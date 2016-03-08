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

package com.moz.fiji.schema.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiAlreadyExistsException;
import com.moz.fiji.schema.FijiMetaTable;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.avro.MetadataBackup;
import com.moz.fiji.schema.avro.TableBackup;
import com.moz.fiji.schema.avro.TableLayoutBackupEntry;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.impl.hbase.HBaseMetaTable;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * The MetadataRestorer is responsible for backing up and restoring the metadata stored in Fiji's
 * Meta, System, and Schema tables, including information about table layouts, schemas, and system
 * variables.
 *
 * <p>To create a backup file, use {@link MetadataRestorer#exportMetadata(String, Fiji)} a filename
 * and a Fiji instance. Backups are stored as {@link MetadataBackup} records. To restore a Fiji
 * instance from a MetadataBackup record, use the various public restore methods.</p>
 */
@ApiAudience.Private
public final class MetadataRestorer {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataRestorer.class);

  /**
   * Exports all Fiji metadata to an Avro file with the specified filename.
   *
   * @param outputFile the output filename. This file must not exist.
   * @param fiji the Fiji instance to backup.
   * @throws IOException when an error communicating with HBase or the filesystem occurs.
   */
  public void exportMetadata(String outputFile, Fiji fiji) throws IOException {
    Preconditions.checkNotNull(outputFile);
    Preconditions.checkNotNull(fiji);

    final File file = new File(outputFile);
    if (file.exists()) {
      throw new IOException("Output file '" + outputFile + "' already exists. Won't overwrite.");
    }
    final MetadataBackup backup = MetadataBackup.newBuilder()
        .setLayoutVersion(fiji.getSystemTable().getDataVersion().toString())
        .setSystemTable(fiji.getSystemTable().toBackup())
        .setSchemaTable(fiji.getSchemaTable().toBackup())
        .setMetaTable(fiji.getMetaTable().toBackup())
        .build();

    // Now write out the file itself.
    final DatumWriter<MetadataBackup> datumWriter =
      new SpecificDatumWriter<MetadataBackup>(MetadataBackup.class);
    final DataFileWriter<MetadataBackup> fileWriter =
      new DataFileWriter<MetadataBackup>(datumWriter);
    try {
      fileWriter.create(backup.getSchema(), file);
      fileWriter.append(backup);
    } finally {
      ResourceUtils.closeOrLog(fileWriter);
    }
  }

  /**
   * Restores the specified table definition from the metadata backup into the running Fiji
   * instance.
   *
   * @param tableName the name of the table to restore.
   * @param tableBackup the deserialized backup of the TableLayout to restore.
   * @param metaTable the MetaTable of the connected Fiji instance.
   * @param fiji the connected Fiji instance.
   * @throws IOException if there is an error communicating with HBase
   */
  private void restoreTable(
    String tableName,
    TableBackup tableBackup,
    FijiMetaTable metaTable,
    Fiji fiji)
    throws IOException {
    Preconditions.checkNotNull(tableBackup);
    Preconditions.checkNotNull(tableBackup.getTableLayoutsBackup());

    List<TableLayoutBackupEntry> layouts = tableBackup.getTableLayoutsBackup().getLayouts();
    Preconditions.checkArgument(!layouts.isEmpty(),
        "Backup for table '%s' contains no layout.", tableName);
    LOG.info("Creating table '{}'.", tableName);

    // The initial entry is the entry with the lowest timestamp.
    long lowestTimestamp = KConstants.END_OF_TIME;
    TableLayoutBackupEntry initialEntry = null;
    for (TableLayoutBackupEntry entry : layouts) {
      if (entry.getTimestamp() < lowestTimestamp) {
        lowestTimestamp = entry.getTimestamp();
        initialEntry = entry;
      }
    }

    try {
      fiji.createTable(initialEntry.getLayout());
    } catch (FijiAlreadyExistsException kaee) {
      LOG.info("Table already exists in HBase. Continuing with restore operation.");
    }

    LOG.info("Restoring layout history for table '%s' (%d layouts).", tableName,
        tableBackup.getTableLayoutsBackup().getLayouts().size());
    metaTable.restoreLayoutsFromBackup(tableName, tableBackup.getTableLayoutsBackup());
    metaTable.restoreKeyValuesFromBackup(tableName, tableBackup.getKeyValueBackup());
  }


  /**
   * Restores all tables from the metadata backup into the running Fiji instance.
   *
   * @param backup the deserialized backup of the metadata.
   * @param fiji the connected Fiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  public void restoreTables(MetadataBackup backup, Fiji fiji) throws IOException {
    final HBaseFactory hbaseFactory = HBaseFactory.Provider.get();
    final HBaseAdmin hbaseAdmin =
        hbaseFactory.getHBaseAdminFactory(fiji.getURI()).create(fiji.getConf());

    final FijiMetaTable metaTable = fiji.getMetaTable();
    try {
      HBaseMetaTable.uninstall(hbaseAdmin, fiji.getURI());
      HBaseMetaTable.install(hbaseAdmin, fiji.getURI());

      final Map<String, TableBackup> tables = backup.getMetaTable().getTables();
      for (Map.Entry<String, TableBackup> layoutEntry : tables.entrySet()) {
        final String tableName = layoutEntry.getKey();
        LOG.debug("Found table backup entry for " + tableName);
        final TableBackup tableBackup = layoutEntry.getValue();
        restoreTable(tableName, tableBackup, metaTable, fiji);
      }
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

   /**
   * Restores all SchemaTable entries from the metadata backup.
   *
   * @param backup the deserialized backup of the metadata.
   * @param fiji the connected Fiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  public void restoreSchemas(MetadataBackup backup, Fiji fiji) throws IOException {
    // Restore all Schema table entries in the file.
    final FijiSchemaTable schemaTable = fiji.getSchemaTable();
    LOG.info("Restoring schema table entries...");
    schemaTable.fromBackup(backup.getSchemaTable());
    LOG.info("Restored " + backup.getSchemaTable().getEntries().size() + " entries.");
  }

  /**
   * Restores all SystemTable entries from the backup.
   *
   * @param backup the deserialized backup of the metadata.
   * @param fiji the connected Fiji instance.
   * @throws IOException if there is an error communicating with HBase.
   */
  public void restoreSystemVars(MetadataBackup backup, Fiji fiji) throws IOException {
    // Restore all System table entries from the file.
    final FijiSystemTable systemTable = fiji.getSystemTable();
    LOG.info("Restoring system table entries...");
    systemTable.fromBackup(backup.getSystemTable());
    LOG.info(String.format("Restored %d entries.", backup.getSystemTable().getEntries().size()));
  }
}
