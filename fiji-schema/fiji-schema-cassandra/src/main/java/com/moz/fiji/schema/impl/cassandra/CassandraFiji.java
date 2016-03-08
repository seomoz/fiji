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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiAlreadyExistsException;
import com.moz.fiji.schema.FijiMetaTable;
import com.moz.fiji.schema.FijiNotInstalledException;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.RowKeyFormat;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.cassandra.CassandraFijiURI;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.impl.Versions;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout;
import com.moz.fiji.schema.layout.impl.InstanceMonitor;
import com.moz.fiji.schema.security.CassandraFijiSecurityManager;
import com.moz.fiji.schema.security.FijiSecurityException;
import com.moz.fiji.schema.security.FijiSecurityManager;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.VersionInfo;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

/**
 * Fiji instance class that contains configuration and table information.
 * Multiple instances of Fiji can be installed onto a single C* cluster.
 * This class represents a single one of those instances.
 *
 * <p>
 *   An opened Fiji instance ignores changes made to the system version, as seen by
 *   {@code Fiji.getSystemTable().getDataVersion()}.
 *   If the system version is modified, the opened Fiji instance should be closed and replaced with
 *   a new Fiji instance.
 * </p>
 */
@ApiAudience.Private
public final class CassandraFiji implements Fiji {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFiji.class);

  /** Factory for CassandraTableInterface instances. */
  private final CassandraAdmin mAdmin;

  /** URI for this CassandraFiji instance. */
  private final CassandraFijiURI mURI;

  /** States of a Fiji instance. */
  private static enum State {
    /**
     * Initialization begun but not completed.  Retain counter and ResourceTracker counters
     * have not been incremented yet.
     */
    UNINITIALIZED,
    /**
     * Finished initialization.  Both retain counters and ResourceTracker counters have been
     * incremented.  Resources are successfully opened and this Fiji's methods may be used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this Fiji instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Retain counter. When decreased to 0, the C* Fiji may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /** ZooKeeper client for this Fiji instance. */
  private final CuratorFramework mZKClient;

  /** Provides table layout updates and user registrations. */
  private final InstanceMonitor mInstanceMonitor;

  /**
   * Cached copy of the system version, oblivious to system table mutation while the connection to
   * this Fiji instance lives.
   * Internally, the Fiji instance must use this version instead of
   * {@code getSystemTable().getDataVersion()} to avoid inconsistent behaviors.
   */
  private final ProtocolVersion mSystemVersion;

  /** The schema table for this fiji instance, or null if it has not been opened yet. */
  private final CassandraSchemaTable mSchemaTable;

  /** The system table for this fiji instance. The system table is always open. */
  private final CassandraSystemTable mSystemTable;

  /** The meta table for this fiji instance, or null if it has not been opened yet. */
  private final CassandraMetaTable mMetaTable;

  /**
   * The security manager for this instance, lazily initialized through {@link #getSecurityManager}.
   */
  private FijiSecurityManager mSecurityManager = null;

  /**
   * Creates a new <code>CassandraFiji</code> instance.
   *
   * <p> Should only be used by Fiji.Factory.open().
   * <p> Caller does not need to use retain(), but must call release() when done with it.
   *
   * @param fijiURI the FijiURI.
   * @param admin CassandraAdmin wrapper around open C* session.
   * @throws java.io.IOException on I/O error.
   */
  CassandraFiji(CassandraFijiURI fijiURI, CassandraAdmin admin) throws IOException {

    // Validate arguments.
    mAdmin = Preconditions.checkNotNull(admin);
    mURI = Preconditions.checkNotNull(fijiURI);

    // Check for an instance name.
    Preconditions.checkArgument(mURI.getInstance() != null,
        "FijiURI '%s' does not specify a Fiji instance name.", mURI);

    LOG.debug(
        "Opening Fiji instance {} with client software version {} and client data version {}.",
        mURI, VersionInfo.getSoftwareVersion(), VersionInfo.getClientDataVersion());

    try {
      mSystemTable = new CassandraSystemTable(mURI, mAdmin);
    } catch (FijiNotInstalledException kie) {
      // Some clients handle this unchecked Exception so do the same here.
      close();
      throw kie;
    }

    mSchemaTable = new CassandraSchemaTable(mAdmin, mURI);
    mMetaTable = new CassandraMetaTable(mURI, mAdmin, mSchemaTable);

    LOG.debug("Fiji instance '{}' is now opened.", mURI);

    mSystemVersion = mSystemTable.getDataVersion();
    LOG.debug("Fiji instance '{}' has data version '{}'.", mURI, mSystemVersion);

    // Make sure the data version for the client matches the cluster.
    LOG.debug("Validating version for Fiji instance '{}'.", mURI);
    try {
      VersionInfo.validateVersion(mSystemTable);
    } catch (IOException ioe) {
      // If an IOException occurred the object will not be constructed so need to clean it up.
      close();
      throw ioe;
    } catch (FijiNotInstalledException kie) {
      // Some clients handle this unchecked Exception so do the same here.
      close();
      throw kie;
    }

    if (mSystemVersion.compareTo(Versions.MIN_SYS_VER_FOR_LAYOUT_VALIDATION) >= 0) {
      // system-2.0 clients must connect to ZooKeeper:
      //  - to register themselves as table users;
      //  - to receive table layout updates.
      mZKClient = ZooKeeperUtils.getZooKeeperClient(mURI);
    } else {
      // system-1.x clients do not need a ZooKeeper connection.
      mZKClient = null;
    }

    mInstanceMonitor = new InstanceMonitor(
        mSystemVersion,
        mURI,
        mSchemaTable,
        mMetaTable,
        mZKClient);
    mInstanceMonitor.start();

    mRetainCount.set(1);
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open Fiji instance in state %s.", oldState);

    ResourceTracker.get().registerResource(this);
  }

  /**
   * <p>
   *   Ensures that a table is not created or modified to enable layout validation without the
   *   requisite system version.
   * </p>
   *
   * <p>
   *   Throws an exception if a table layout has validation enabled, but the overall instance data
   *   version is too low to support table layout validation.
   * </p>
   *
   * <p>
   *   Table layouts with layout version <tt>layout-1.3.0</tt> or higher must be applied to systems
   *   with data version <tt>system-2.0</tt> or higher. A layout of 1.3 or above in system-1.0
   *   environment will trigger an exception in this method.
   * </p>
   *
   * <p>
   *   Older layout versions may be applied in <tt>system-1.0</tt> or <tt>system-2.0</tt>
   *   environments; such layouts are ignored by this method.
   * </p>
   *
   * @param layout the table layout for which to ensure compatibility.
   * @throws java.io.IOException in case of an error reading from the system table.
   * @throws com.moz.fiji.schema.layout.InvalidLayoutException if the layout and system versions are
   * incompatible.
   */
  private void ensureValidationCompatibility(TableLayoutDesc layout) throws IOException {
    final ProtocolVersion layoutVersion = ProtocolVersion.parse(layout.getVersion());
    final ProtocolVersion systemVersion = getSystemTable().getDataVersion();

    if ((layoutVersion.compareTo(Versions.LAYOUT_VALIDATION_VERSION) >= 0)
        && (systemVersion.compareTo(Versions.MIN_SYS_VER_FOR_LAYOUT_VALIDATION) < 0)) {
      throw new InvalidLayoutException(
          String.format("Layout version: %s not supported by system version: %s",
              layoutVersion, systemVersion));
    }
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    throw new UnsupportedOperationException(
        "Cassandra-backed Fiji instances do not support Hadoop configuration.");
  }

  /** {@inheritDoc} */
  @Override
  public CassandraFijiURI getURI() {
    return mURI;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized FijiSchemaTable getSchemaTable() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get schema table for Fiji instance %s in state %s.", this, state);
    return mSchemaTable;
  }

  /** {@inheritDoc} */
  @Override
  public FijiSystemTable getSystemTable() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get system table for Fiji instance %s in state %s.", this, state);
    return mSystemTable;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized FijiMetaTable getMetaTable() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get meta table for Fiji instance %s in state %s.", this, state);
    return mMetaTable;
  }

  /**
   * Gets the current CassandraAdmin instance for this Fiji. This method will open a new
   * CassandraAdmin if one doesn't exist already.
   *
   * @throws java.io.IOException If there is an error opening the CassandraAdmin.
   * @return The current CassandraAdmin instance for this Fiji.
   */
  public synchronized CassandraAdmin getCassandraAdmin() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get HBase admin for Fiji instance %s in state %s.", this, state);
    return mAdmin;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isSecurityEnabled() throws IOException {
    return mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) >= 0;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized FijiSecurityManager getSecurityManager() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get security manager for Fiji instance %s in state %s.", this, state);
    if (null == mSecurityManager) {
      if (isSecurityEnabled()) {
        mSecurityManager = CassandraFijiSecurityManager.create(mURI);
      } else {
        throw new FijiSecurityException(
            String.format(
                "Can not create a FijiSecurityManager for security version %s."
                  + "  Version must be %s or higher.",
                mSystemTable.getSecurityVersion(), Versions.MIN_SECURITY_VERSION));
      }
    }
    return mSecurityManager;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraFijiTable openTable(String tableName) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open table in Fiji instance %s in state %s.", this, state);
    return new CassandraFijiTable(
        this,
        tableName,
        mAdmin,
        mInstanceMonitor.getTableLayoutMonitor(tableName));
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void createTable(String tableName, FijiTableLayout tableLayout)
      throws IOException {
    if (!tableName.equals(tableLayout.getName())) {
      throw new RuntimeException(String.format(
          "Table name from layout descriptor '%s' does match table name '%s'.",
          tableLayout.getName(), tableName));
    }

    createTable(tableLayout.getDesc());
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(final TableLayoutDesc tableLayout) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot create table in Fiji instance %s in state %s.", this, state);

    final FijiURI tableURI = FijiURI.newBuilder(mURI).withTableName(tableLayout.getName()).build();
    LOG.debug("Creating Fiji table '{}'.", tableURI);

    ensureValidationCompatibility(tableLayout);

    // If security is enabled, apply the permissions to the new table.
    if (isSecurityEnabled()) {
      getSecurityManager().lock();
      try {
        createTableUnchecked(tableLayout);
        getSecurityManager().applyPermissionsToNewTable(tableURI);
      } finally {
        getSecurityManager().unlock();
      }
    } else {
      createTableUnchecked(tableLayout);
    }
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void createTable(
      final String tableName,
      final FijiTableLayout tableLayout,
      final int numRegions
  ) throws IOException {
    Preconditions.checkArgument(tableName.equals(tableLayout.getName()),
        "Table name from layout descriptor '%s' does match table name '%s'.",
        tableLayout.getName(), tableName);
    createTable(tableLayout.getDesc(), numRegions);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(
      final TableLayoutDesc tableLayout,
      final int numRegions
  ) throws IOException {
    LOG.warn("Fiji Cassandra does not support creating tables with regions.");
    createTable(tableLayout);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void createTable(
      final String tableName,
      final FijiTableLayout tableLayout,
      final byte[][] splitKeys
  ) throws IOException {
    Preconditions.checkArgument(tableName.equals(tableLayout.getName()),
        "Table name from layout descriptor '%s' does match table name '%s'.",
        tableLayout.getName(), tableName);

    createTable(tableLayout.getDesc());
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableLayoutDesc tableLayout, byte[][] splitKeys) throws IOException {
    LOG.warn("Fiji Cassandra does not support creating tables with regions.");
    createTable(tableLayout);


  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public FijiTableLayout modifyTableLayout(String tableName, TableLayoutDesc update)
      throws IOException {
    if (!tableName.equals(update.getName())) {
      throw new InvalidLayoutException(String.format(
          "Name of table in descriptor '%s' does not match table name '%s'.",
          update.getName(), tableName));
    }

    return modifyTableLayout(update);
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableLayout modifyTableLayout(TableLayoutDesc update) throws IOException {
    return modifyTableLayout(update, false, null);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public FijiTableLayout modifyTableLayout(
      String tableName,
      TableLayoutDesc update,
      boolean dryRun,
      PrintStream printStream)
      throws IOException {
    if (!tableName.equals(update.getName())) {
      throw new InvalidLayoutException(String.format(
          "Name of table in descriptor '%s' does not match table name '%s'.",
          update.getName(), tableName));
    }

    return modifyTableLayout(update, dryRun, printStream);
  }

  // CSOFF: MethodLength
  /** {@inheritDoc} */
  @Override
  // TODO: Implement C* version
  public FijiTableLayout modifyTableLayout(
      TableLayoutDesc update,
      boolean dryRun,
      PrintStream printStream)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot modify table layout in Fiji instance %s in state %s.", this, state);
    Preconditions.checkNotNull(update);

    ensureValidationCompatibility(update);

    // Note that a Cassandra table layout modification should never require a schema change
    // in the underlying Cassandra table, unless we are adding or removing a counter.

    if (dryRun && (null == printStream)) {
      printStream = System.out;
    }

    final FijiMetaTable metaTable = getMetaTable();

    final String tableName = update.getName();
    // Throws a FijiTableNotFoundException if there is no table.
    metaTable.getTableLayout(tableName);

    final FijiURI tableURI = FijiURI.newBuilder(mURI).withTableName(tableName).build();
    LOG.debug("Applying layout update {} on table {}", update, tableURI);

    FijiTableLayout newLayout = null;

    if (dryRun) {
      // Process column ids and perform validation, but don't actually update the meta table.
      final List<FijiTableLayout> layouts = metaTable.getTableLayoutVersions(tableName, 1);
      final FijiTableLayout currentLayout = layouts.isEmpty() ? null : layouts.get(0);
      newLayout = FijiTableLayout.createUpdatedLayout(update, currentLayout);
    } else {
      // Actually set it.
      if (mSystemVersion.compareTo(Versions.SYSTEM_2_0) >= 0) {
        try {
          // Use ZooKeeper to inform all watchers that a new table layout is available.
          final CassandraTableLayoutUpdater updater =
              new CassandraTableLayoutUpdater(this, tableURI, update);
          try {
            updater.update();
            newLayout = updater.getNewLayout();
          } finally {
            updater.close();
          }
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
      } else {
        // System versions before system-2.0 do not enforce table layout update consistency or
        // validation.
        newLayout = metaTable.updateTableLayout(tableName, update);
      }
    }
    Preconditions.checkState(newLayout != null);

    if (dryRun) {
      printStream.println("This table layout is valid.");
    }

    if (dryRun) {
      printStream.println("No changes possible for Cassandra-backed Fiji tables.");
    }

    return newLayout;
  }
  // CSON: MethodLength

  /** {@inheritDoc} */
  @Override
  public void deleteTable(String tableName) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete table in Fiji instance %s in state %s.", this, state);
    // Delete from Cassandra.
    final FijiURI tableURI = FijiURI.newBuilder(mURI).withTableName(tableName).build();

    final FijiTableLayout layout = mMetaTable.getTableLayout(tableName);
    final List<ResultSetFuture> futures =
        Lists.newArrayListWithCapacity(layout.getLocalityGroups().size());
    for (LocalityGroupLayout localityGroup : layout.getLocalityGroups()) {
      final String delete =
          CQLUtils.getDropTableStatement(
              CassandraTableName.getLocalityGroupTableName(tableURI, localityGroup.getId()));

      futures.add(mAdmin.executeAsync(delete));
    }

    // Delete from the meta table
    getMetaTable().deleteTable(tableName);

    for (ResultSetFuture future : futures) {
      future.getUninterruptibly();
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getTableNames() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get table names in Fiji instance %s in state %s.", this, state);
    return getMetaTable().listTables();
  }

  /**
   * Releases all the resources used by this Fiji instance.
   *
   * @throws java.io.IOException on I/O error.
   */
  private void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN || oldState == State.UNINITIALIZED,
        "Cannot close Fiji instance %s in state %s.", this, oldState);

    LOG.debug("Closing {}.", this);

    ResourceUtils.closeOrLog(mInstanceMonitor);
    ResourceUtils.closeOrLog(mMetaTable);
    ResourceUtils.closeOrLog(mSystemTable);
    ResourceUtils.closeOrLog(mSchemaTable);
    ResourceUtils.closeOrLog(mAdmin);

    synchronized (this) {
      ResourceUtils.closeOrLog(mSecurityManager);
    }

    ResourceUtils.closeOrLog(mZKClient);

    if (oldState != State.UNINITIALIZED) {
      ResourceTracker.get().unregisterResource(this);
    }

    LOG.debug("{} closed.", this);
  }

  /** {@inheritDoc} */
  @Override
  public Fiji retain() {
    LOG.debug("Retaining {}.", this);
    final int counter = mRetainCount.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain Fiji instance %s: retain counter was %s.", this, counter);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    LOG.debug("Releasing {}", this);
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release Fiji instance %s: retain counter is now %s.", this, counter);
    if (counter == 0) {
      close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (null == obj) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!getClass().equals(obj.getClass())) {
      return false;
    }
    final Fiji other = (Fiji) obj;

    // Equal if the two instances have the same URI:
    return mURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraFiji.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mURI)
        .add("retain-count", mRetainCount)
        .add("state", mState.get())
        .toString();
  }

  /**
   * Returns the ZooKeeper client for this Fiji instance.
   *
   * @return the ZooKeeper client for this Fiji instance.
   *     Null if the data version &le; {@code system-2.0}.
   */
  CuratorFramework getZKClient() {
    return mZKClient;
  }

  /**
   * Creates a Fiji table in a Cassandra instance, without checking for validation compatibility and
   * without applying permissions.
   *
   * @param tableLayout The initial layout of the table (with unassigned column ids).
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   */
  private void createTableUnchecked(TableLayoutDesc tableLayout) throws IOException {
    final FijiURI tableURI = FijiURI.newBuilder(mURI).withTableName(tableLayout.getName()).build();
    CassandraTableLayoutUpdater.validateCassandraTableLayout(tableLayout);

    // This will validate the layout and may throw an InvalidLayoutException.
    final FijiTableLayout layout = FijiTableLayout.newLayout(tableLayout);

    if (getMetaTable().tableExists(tableLayout.getName())) {
      throw new FijiAlreadyExistsException(
          String.format("Fiji table '%s' already exists.", tableURI), tableURI);
    }

    if (tableLayout.getKeysFormat() instanceof RowKeyFormat) {
      throw new InvalidLayoutException(
          "CassandraFiji does not support 'RowKeyFormat', instead use 'RowKeyFormat2'.");
    }

    getMetaTable().updateTableLayout(tableLayout.getName(), tableLayout);

    if (mSystemVersion.compareTo(Versions.SYSTEM_2_0) >= 0) {
      // system-2.0 clients retrieve the table layout from ZooKeeper as a stream of notifications.
      // Invariant: ZooKeeper hold the most recent layout of the table.
      ZooKeeperUtils.setTableLayout(mZKClient, tableURI, layout.getDesc().getLayoutId());
    }

    final List<CassandraTableName> tables =
        Lists.newArrayListWithCapacity(tableLayout.getLocalityGroups().size());

    for (LocalityGroupLayout localityGroup : layout.getLocalityGroups()) {
      tables.add(CassandraTableName.getLocalityGroupTableName(tableURI, localityGroup.getId()));
    }

    final List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(tables.size());

    for (CassandraTableName table : tables) {
      final String create = CQLUtils.getCreateLocalityGroupTableStatement(table, layout);
      futures.add(mAdmin.executeAsync(create));
    }

    for (ResultSetFuture future : futures) {
      future.getUninterruptibly();
    }
  }
}
