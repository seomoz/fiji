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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.FijiMetaTable;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiTableKeyValueDatabase;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.KeyValueBackup;
import com.moz.fiji.schema.avro.MetaTableBackup;
import com.moz.fiji.schema.avro.TableBackup;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.avro.TableLayoutsBackup;
import com.moz.fiji.schema.hbase.FijiManagedHBaseTableName;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayoutDatabase;
import com.moz.fiji.schema.layout.impl.HBaseTableLayoutDatabase;

/**
 * An implementation of the FijiMetaTable that uses the 'fiji-meta' HBase table as the backing
 * store.
 */
@ApiAudience.Private
public final class HBaseMetaTable implements FijiMetaTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseMetaTable.class);

  /** The HBase column family that will store table layout specific metadata. */
  private static final String LAYOUT_COLUMN_FAMILY = "layout";
  /** The HBase column family that will store user defined metadata. */
  private static final String META_COLUMN_FAMILY = "meta";

  /** URI of the Fiji instance this meta-table belongs to. */
  private final FijiURI mFijiURI;

  /** The HBase table that stores Fiji metadata. */
  private final HTableInterface mTable;

  /** States of a SchemaTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SchemaTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** The layout table that we delegate the work of storing table layout metadata to. */
  private final FijiTableLayoutDatabase mTableLayoutDatabase;

  /** The table we delegate storing per table meta data, in the form of key value pairs.  */
  private final FijiTableKeyValueDatabase<?> mTableKeyValueDatabase;
  // TODO: Make FijiTableLayoutDatabase thread-safe,
  //     so we can call HBaseMetaTable thread-safe, too.

  /**
   * Creates an HTableInterface for the specified table.
   *
   * @param fijiURI the FijiURI.
   * @param conf Hadoop configuration.
   * @param factory HTableInterface factory to use.
   * @return a new HTableInterface for the specified table.
   * @throws IOException on I/O error.
   */
  public static HTableInterface newMetaTable(
      FijiURI fijiURI,
      Configuration conf,
      HTableInterfaceFactory factory)
      throws IOException {
    final String hbaseTableName =
        FijiManagedHBaseTableName.getMetaTableName(fijiURI.getInstance()).toString();
    return factory.create(conf, hbaseTableName);
  }

  /**
   * Create a connection to a Fiji meta table backed by an HTable within HBase.
   *
   * @param fijiURI URI of the Fiji instance this meta-table belongs to.
   * @param conf The Hadoop configuration.
   * @param schemaTable The Fiji schema table.
   * @param factory HTableInterface factory.
   * @throws IOException If there is an error.
   */
  HBaseMetaTable(
      FijiURI fijiURI,
      Configuration conf,
      FijiSchemaTable schemaTable,
      HTableInterfaceFactory factory)
      throws IOException {
    this(fijiURI, newMetaTable(fijiURI, conf, factory), schemaTable);
  }

  /**
   * Create a connection to a Fiji meta table backed by an HTable within HBase.
   *
   * <p>This class takes ownership of the HTable. It will be closed when this instance is
   * closed.</p>
   *
   * @param fijiURI URI of the Fiji instance this meta-table belongs to.
   * @param htable The HTable to use for storing Fiji meta data.
   * @param schemaTable The Fiji schema table.
   * @throws IOException If there is an error.
   */
  private HBaseMetaTable(
      FijiURI fijiURI,
      HTableInterface htable,
      FijiSchemaTable schemaTable)
      throws IOException {
    this(
        fijiURI,
        htable,
        new HBaseTableLayoutDatabase(fijiURI, htable, LAYOUT_COLUMN_FAMILY, schemaTable),
        new HBaseTableKeyValueDatabase(htable, META_COLUMN_FAMILY));
  }

  /**
   * Create a connection to a Fiji meta table backed by an HTable within HBase.
   *
   * <p>This class takes ownership of the HTable. It will be closed when this instance is
   * closed.</p>
   *
   * @param fijiURI URI of the Fiji instance this meta-table belongs to.
   * @param htable The HTable to use for storing Fiji meta data.
   * @param tableLayoutDatabase A database of table layouts to delegate layout storage to.
   * @param tableKeyValueDatabase A database of key-value pairs to delegate metadata storage to.
   */
  private HBaseMetaTable(
      FijiURI fijiURI,
      HTableInterface htable,
      FijiTableLayoutDatabase tableLayoutDatabase,
      FijiTableKeyValueDatabase<?> tableKeyValueDatabase) {
    mFijiURI = fijiURI;
    mTable = htable;
    mTableLayoutDatabase = tableLayoutDatabase;
    mTableKeyValueDatabase = tableKeyValueDatabase;
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open MetaTable instance in state %s.", oldState);
    ResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void deleteTable(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete table from MetaTable instance in state %s.", state);
    mTableLayoutDatabase.removeAllTableLayoutVersions(table);
    mTableKeyValueDatabase.removeAllValues(table);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized FijiTableLayout updateTableLayout(String table, TableLayoutDesc layoutUpdate)
    throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot update table layout in MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.updateTableLayout(table, layoutUpdate);
  }
  /** {@inheritDoc} */
  @Override
  public synchronized FijiTableLayout getTableLayout(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get table layout from MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.getTableLayout(table);
  }
  /** {@inheritDoc} */
  @Override
  public synchronized List<FijiTableLayout> getTableLayoutVersions(String table, int numVersions)
    throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get table layout versions from MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.getTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableMap<Long, FijiTableLayout> getTimedTableLayoutVersions(String table,
    int numVersions) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get timed table layout versions from MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.getTimedTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeAllTableLayoutVersions(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot remove all table layout versions from MetaTable instance in state %s.", state);
    mTableLayoutDatabase.removeAllTableLayoutVersions(table);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeRecentTableLayoutVersions(String table, int numVersions)
    throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot remove recent table layout versions from MetaTable instance in state %s.", state);
    mTableLayoutDatabase.removeRecentTableLayoutVersions(table, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized List<String> listTables() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot list tables in MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.listTables();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean tableExists(String tableName) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot check if table exists in MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.tableExists(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close MetaTable instance in state %s.", oldState);
    ResourceTracker.get().unregisterResource(this);
    mTable.close();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized byte[] getValue(String table, String key) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get value from MetaTable instance in state %s.", state);
    return mTableKeyValueDatabase.getValue(table, key);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized FijiMetaTable putValue(String table, String key, byte[] value)
    throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put value into MetaTable instance in state %s.", state);
    mTableKeyValueDatabase.putValue(table, key, value);
    return this; // Don't expose the delegate object.
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void removeValues(String table, String key) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get removed values from MetaTable instance in state %s.", state);
    mTableKeyValueDatabase.removeValues(table, key);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> tableSet() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get table set from MetaTable instance in state %s.", state);
    return mTableKeyValueDatabase.tableSet();
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> keySet(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get key set from MetaTable instance in state %s.", state);
    return mTableKeyValueDatabase.keySet(table);
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllValues(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot remove all values from MetaTable instance in state %s.", state);
    mTableKeyValueDatabase.removeAllValues(table);
  }

  /**
   * Install the meta table into a Fiji instance.
   *
   * @param admin The HBase Admin interface for the HBase cluster to install into.
   * @param uri The uri of the Fiji instance to install.
   * @throws IOException If there is an error.
   */
  public static void install(HBaseAdmin admin, FijiURI uri)
    throws IOException {
    HTableDescriptor tableDescriptor = new HTableDescriptor(
      FijiManagedHBaseTableName.getMetaTableName(uri.getInstance()).toString());
    tableDescriptor.addFamily(
      HBaseTableLayoutDatabase.getHColumnDescriptor(LAYOUT_COLUMN_FAMILY));
    tableDescriptor.addFamily(
      HBaseTableLayoutDatabase.getHColumnDescriptor(META_COLUMN_FAMILY));
    admin.createTable(tableDescriptor);
  }

  /**
   * Removes the meta table from HBase.
   *
   * @param admin The HBase admin object.
   * @param uri The uri of the Fiji instance to uninstall.
   * @throws IOException If there is an error.
   */
  public static void uninstall(HBaseAdmin admin, FijiURI uri)
    throws IOException {
    String tableName = FijiManagedHBaseTableName.getMetaTableName(uri.getInstance()).toString();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public MetaTableBackup toBackup() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot backup MetaTable instance in state %s.", state);
    Map<String, TableBackup> backupEntries = new HashMap<String, TableBackup>();
    List<String> tables = listTables();
    for (String table : tables) {
      TableLayoutsBackup layouts = mTableLayoutDatabase.layoutsToBackup(table);
      KeyValueBackup keyValues = mTableKeyValueDatabase.keyValuesToBackup(table);
      final TableBackup tableBackup = TableBackup.newBuilder()
          .setName(table)
          .setTableLayoutsBackup(layouts)
          .setKeyValueBackup(keyValues)
          .build();
      backupEntries.put(table, tableBackup);
    }
    return MetaTableBackup.newBuilder().setTables(backupEntries).build();
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(MetaTableBackup backup) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore backup to MetaTable instance in state %s.", state);
    LOG.info(String.format("Restoring meta table from backup with %d entries.",
        backup.getTables().size()));
    for (Map.Entry<String, TableBackup> tableEntry: backup.getTables().entrySet()) {
      final String tableName = tableEntry.getKey();
      final TableBackup tableBackup = tableEntry.getValue();
      Preconditions.checkState(tableName.equals(tableBackup.getName()), String.format(
          "Inconsistent table backup: entry '%s' does not match table name '%s'.",
          tableName, tableBackup.getName()));
      restoreLayoutsFromBackup(tableName, tableBackup.getTableLayoutsBackup());
      restoreKeyValuesFromBackup(tableName, tableBackup.getKeyValueBackup());
    }
    mTable.flushCommits();
    LOG.info("Flushing commits to table '{}'", Bytes.toString(mTable.getTableName()));
  }

  /** {@inheritDoc} */
  @Override
  public TableLayoutsBackup layoutsToBackup(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get layouts to backup from MetaTable instance in state %s.", state);
    return mTableLayoutDatabase.layoutsToBackup(table);
  }

  /** {@inheritDoc} */
  @Override
  public List<byte[]> getValues(String table, String key, int numVersions) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get values from MetaTable instance in state %s.", state);
    return mTableKeyValueDatabase.getValues(table, key, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, byte[]> getTimedValues(String table, String key, int numVersions)
    throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get timed values from MetaTable instance in state %s.", state);
    return mTableKeyValueDatabase.getTimedValues(table, key, numVersions);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueBackup keyValuesToBackup(String table) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get key values to backup from MetaTable instance in state %s.", state);
    return mTableKeyValueDatabase.keyValuesToBackup(table);
  }

  /** {@inheritDoc} */
  @Override
  public void restoreKeyValuesFromBackup(String table, KeyValueBackup tableBackup) throws
      IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore key values from backup from MetaTable instance in state %s.", state);
    mTableKeyValueDatabase.restoreKeyValuesFromBackup(table, tableBackup);
  }

  @Override
  public void restoreLayoutsFromBackup(String tableName, TableLayoutsBackup tableBackup) throws
      IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore layouts from backup from MetaTable instance in state %s.", state);
    mTableLayoutDatabase.restoreLayoutsFromBackup(tableName, tableBackup);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseMetaTable.class)
        .add("uri", mFijiURI)
        .add("state", mState.get())
        .toString();
  }
}
