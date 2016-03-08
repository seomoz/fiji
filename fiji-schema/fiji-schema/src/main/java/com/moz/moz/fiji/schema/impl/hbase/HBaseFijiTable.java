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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiReaderFactory;
import com.moz.fiji.schema.FijiRegion;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableAnnotator;
import com.moz.fiji.schema.FijiTableNotFoundException;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.FijiWriterFactory;
import com.moz.fiji.schema.avro.RowKeyFormat;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.hbase.FijiManagedHBaseTableName;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.impl.LayoutConsumer.Registration;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.TableLayoutMonitor;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.VersionInfo;

/**
 * <p>A FijiTable that exposes the underlying HBase implementation.</p>
 *
 * <p>Within the internal Fiji code, we use this class so that we have
 * access to the HTable interface.  Methods that Fiji clients should
 * have access to should be added to com.moz.fiji.schema.FijiTable.</p>
 */
@ApiAudience.Private
public final class HBaseFijiTable implements FijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFijiTable.class);

  /** String identifying the scheme of a URI to an HDFS path. */
  private static final String HDFS_SCHEME = "hdfs";

  /** String required for requesting an HDFS delegation token. */
  private static final String RENEWER = "renewer";

  /** The fiji instance this table belongs to. */
  private final HBaseFiji mFiji;

  /** The name of this table (the Fiji name, not the HBase name). */
  private final String mName;

  /** URI of this table. */
  private final FijiURI mTableURI;

  /** States of a fiji table instance. */
  private static enum State {
    /**
     * Initialization begun but not completed.  Retain counter and ResourceTracker counters
     * have not been incremented yet.
     */
    UNINITIALIZED,
    /**
     * Finished initialization.  Both retain counters and ResourceTracker counters have been
     * incremented.  Resources are successfully opened and this HBaseFijiTable's methods may be
     * used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this fiji table. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** HTableInterfaceFactory for creating new HTables associated with this FijiTable. */
  private final HTableInterfaceFactory mHTableFactory;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /** Retain counter. When decreased to 0, the HBase FijiTable may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /** Configuration object for new HTables. */
  private final Configuration mConf;

  /** Writer factory for this table. */
  private final FijiWriterFactory mWriterFactory;

  /** Reader factory for this table. */
  private final FijiReaderFactory mReaderFactory;

  /** HConnection used for creating lightweight tables. Should be closed by us. */
  private final HConnection mHConnection;

  /** Name of the HBase table backing this Fiji table. */
  private final String mHBaseTableName;

  /**
   * Monitor for the layout of this table.
   **/
  private final TableLayoutMonitor mLayoutMonitor;

  /**
   * Construct an opened Fiji table stored in HBase.
   *
   * @param fiji The Fiji instance.
   * @param name The name of the Fiji user-space table to open.
   * @param conf The Hadoop configuration object.
   * @param htableFactory A factory that creates HTable objects.
   * @param layoutMonitor a valid TableLayoutMonitor for this table.
   * @throws IOException On an HBase error.
   * @throws FijiTableNotFoundException if the table does not exist.
   */
  HBaseFijiTable(
      HBaseFiji fiji,
      String name,
      Configuration conf,
      HTableInterfaceFactory htableFactory,
      TableLayoutMonitor layoutMonitor
  ) throws IOException {
    mFiji = fiji;
    mFiji.retain();

    mName = name;
    mHTableFactory = htableFactory;
    mConf = conf;
    mTableURI = FijiURI.newBuilder(mFiji.getURI()).withTableName(mName).build();
    LOG.debug("Opening Fiji table '{}' with client version '{}'.",
        mTableURI, VersionInfo.getSoftwareVersion());
    mHBaseTableName =
        FijiManagedHBaseTableName.getFijiTableName(mTableURI.getInstance(), mName).toString();

    if (!mFiji.getTableNames().contains(mName)) {
      closeResources();
      throw new FijiTableNotFoundException(mTableURI);
    }

    mWriterFactory = new HBaseFijiWriterFactory(this);
    mReaderFactory = new HBaseFijiReaderFactory(this);

    mLayoutMonitor = layoutMonitor;
    mEntityIdFactory = createEntityIdFactory(mLayoutMonitor.getLayout());

    mHConnection = HBaseFactory.Provider.get().getHConnection(mFiji);

    // Table is now open and must be released properly:
    mRetainCount.set(1);

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open FijiTable instance in state %s.", oldState);
    ResourceTracker.get().registerResource(this);
  }

  /**
   * Constructs an Entity ID factory from a layout capsule.
   *
   * @param layout layout to construct an entity ID factory from.
   * @return a new entity ID factory as described from the table layout.
   */
  private static EntityIdFactory createEntityIdFactory(final FijiTableLayout layout) {
    final Object format = layout.getDesc().getKeysFormat();
    if (format instanceof RowKeyFormat) {
      return EntityIdFactory.getFactory((RowKeyFormat) format);
    } else if (format instanceof RowKeyFormat2) {
      return EntityIdFactory.getFactory((RowKeyFormat2) format);
    } else {
      throw new RuntimeException("Invalid Row Key format found in Fiji Table: " + format);
    }
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... fijiRowKey) {
    return mEntityIdFactory.getEntityId(fijiRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public Fiji getFiji() {
    return mFiji;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return mName;
  }

  /** {@inheritDoc} */
  @Override
  public FijiURI getURI() {
    return mTableURI;
  }

  /**
   * Register a layout consumer that must be updated before this table will report that it has
   * completed a table layout update.  Sends the first update immediately before returning. The
   * returned registration object must be closed when layout updates are no longer needed.
   *
   * @param consumer the LayoutConsumer to be registered.
   * @return a registration object which must be closed when layout updates are no longer needed.
   * @throws IOException in case of an error updating the LayoutConsumer.
   */
  public Registration registerLayoutConsumer(LayoutConsumer consumer) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot register a new layout consumer to a FijiTable in state %s.", state);
    return mLayoutMonitor.registerLayoutConsumer(consumer);
  }

  /**
   * Get the TableLayoutMonitor which is associated with this HBaseFijiTable.
   *
   * @return the TableLayoutMonitor associated with this HBaseFijiTable.
   */
  public TableLayoutMonitor getTableLayoutMonitor() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get a table layout monitor from a FijiTable in state %s.", state);
    return mLayoutMonitor;
  }

  /**
   * Opens a new connection to the HBase table backing this Fiji table.
   *
   * <p> The caller is responsible for properly closing the connection afterwards. </p>
   * <p>
   *   Note: this does not necessarily create a new HTable instance, but may instead return
   *   an already existing HTable instance from a pool managed by this HBaseFijiTable.
   *   Closing a pooled HTable instance internally moves the HTable instance back into the pool.
   * </p>
   *
   * @return A new HTable associated with this FijiTable.
   * @throws IOException in case of an error.
   */
  public HTableInterface openHTableConnection() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open an HTable connection for a FijiTable in state %s.", state);
    return mHConnection.getTable(mHBaseTableName);
  }

  /**
   * {@inheritDoc}
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should create the column name translator directly from the returned layout.
   */
  @Override
  public FijiTableLayout getLayout() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the layout of a table in state %s.", state);
    return mLayoutMonitor.getLayout();
  }

  /**
   * Get the column name translator for the current layout of this table.  Do not cache this object.
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should use {@link #getLayout()}} and create your own
   * {@link HBaseColumnNameTranslator} to ensure consistent state.
   * @return the column name translator for the current layout of this table.
   */
  public HBaseColumnNameTranslator getColumnNameTranslator() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the column name translator of a table in state %s.", state);
    return HBaseColumnNameTranslator.from(getLayout());
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableReader openTableReader() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table reader on a FijiTable in state %s.", state);
    try {
      return HBaseFijiTableReader.create(this);
    } catch (IOException ioe) {
      throw new FijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableWriter openTableWriter() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table writer on a FijiTable in state %s.", state);
    try {
      return new HBaseFijiTableWriter(this);
    } catch (IOException ioe) {
      throw new FijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FijiReaderFactory getReaderFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the reader factory for a FijiTable in state %s.", state);
    return mReaderFactory;
  }

  /** {@inheritDoc} */
  @Override
  public FijiWriterFactory getWriterFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the writer factory for a FijiTable in state %s.", state);
    return mWriterFactory;
  }

  /**
   * Return the regions in this table as a list.
   *
   * <p>This method was copied from HFileOutputFormat of 0.90.1-cdh3u0 and modified to
   * return FijiRegion instead of ImmutableBytesWritable.</p>
   *
   * @return An ordered list of the table regions.
   * @throws IOException on I/O error.
   */
  @Override
  public List<FijiRegion> getRegions() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the regions for a FijiTable in state %s.", state);
    final HBaseAdmin hbaseAdmin = ((HBaseFiji) getFiji()).getHBaseAdmin();
    final HTableInterface htable = mHTableFactory.create(mConf,  mHBaseTableName);
    try {
      final List<HRegionInfo> regions = hbaseAdmin.getTableRegions(htable.getTableName());
      final List<FijiRegion> result = Lists.newArrayList();

      // If we can get the concrete HTable, we can get location information.
      if (htable instanceof HTable) {
        LOG.debug("Casting HTableInterface to an HTable.");
        final HTable concreteHBaseTable = (HTable) htable;
        for (HRegionInfo region: regions) {
          List<HRegionLocation> hLocations =
              concreteHBaseTable.getRegionsInRange(region.getStartKey(), region.getEndKey());
          result.add(new HBaseFijiRegion(region, hLocations));
        }
      } else {
        LOG.warn("Unable to cast HTableInterface {} to an HTable.  "
            + "Creating Fiji regions without location info.", getURI());
        for (HRegionInfo region: regions) {
          result.add(new HBaseFijiRegion(region));
        }
      }

      return result;

    } finally {
      htable.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Collection<HBaseFijiPartition> getPartitions() throws IOException {
    try (HTableInterface htable = mHTableFactory.create(mConf, mHBaseTableName)) {
      return HBaseFijiPartition.getPartitions((HTable) htable);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableAnnotator openTableAnnotator() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the TableAnnotator for a table in state: %s.", state);
    return new HBaseFijiTableAnnotator(this);
  }

  /**
   * Releases the resources used by this table.
   *
   * @throws IOException on I/O error.
   */
  private void closeResources() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN || oldState == State.UNINITIALIZED,
        "Cannot close FijiTable instance %s in state %s.", this, oldState);
    LOG.debug("Closing HBaseFijiTable '{}'.", this);

    ResourceUtils.closeOrLog(mHConnection);
    ResourceUtils.closeOrLog(mLayoutMonitor);
    ResourceUtils.releaseOrLog(mFiji);
    if (oldState != State.UNINITIALIZED) {
      ResourceTracker.get().unregisterResource(this);
    }

    LOG.debug("HBaseFijiTable '{}' closed.", mTableURI);
  }

  /** {@inheritDoc} */
  @Override
  public FijiTable retain() {
    final int counter = mRetainCount.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain a closed FijiTable %s: retain counter was %s.", mTableURI, counter);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release closed FijiTable %s: retain counter is now %s.", mTableURI, counter);
    if (counter == 0) {
      closeResources();
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
    final FijiTable other = (FijiTable) obj;

    // Equal if the two tables have the same URI:
    return mTableURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mTableURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    String layoutId = mState.get() == State.OPEN
        ? mLayoutMonitor.getLayout().getDesc().getLayoutId()
        : "unknown";
    return Objects.toStringHelper(HBaseFijiTable.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mTableURI)
        .add("retain_counter", mRetainCount.get())
        .add("layout_id", layoutId)
        .add("state", mState.get())
        .toString();
  }

  /**
   * We know that all FijiTables are really HBaseFijiTables
   * instances.  This is a convenience method for downcasting, which
   * is common within the internals of Fiji code.
   *
   * @param fijiTable The Fiji table to downcast to an HBaseFijiTable.
   * @return The given Fiji table as an HBaseFijiTable.
   */
  public static HBaseFijiTable downcast(FijiTable fijiTable) {
    if (!(fijiTable instanceof HBaseFijiTable)) {
      // This should really never happen.  Something is seriously
      // wrong with Fiji code if we get here.
      throw new InternalFijiError(
          "Found a FijiTable object that was not an instance of HBaseFijiTable.");
    }
    return (HBaseFijiTable) fijiTable;
  }

  /**
   * Creates a new HFile loader.
   *
   * @param conf Configuration object for the HFile loader.
   * @return the new HFile loader.
   */
  private static LoadIncrementalHFiles createHFileLoader(Configuration conf) {
    try {
      return new LoadIncrementalHFiles(conf); // throws Exception
    } catch (Exception exn) {
      throw new InternalFijiError(exn);
    }
  }

  /**
   * Loads partitioned HFiles directly into the regions of this Fiji table.
   *
   * @param hfilePath Path of the HFiles to load.
   * @throws IOException on I/O error.
   */
  public void bulkLoad(Path hfilePath) throws IOException {
    final LoadIncrementalHFiles loader = createHFileLoader(mConf);

    final String hFileScheme = hfilePath.toUri().getScheme();
    Token<DelegationTokenIdentifier> hdfsDelegationToken = null;

    // If we're bulk loading from a secure HDFS, we should request and forward a delegation token.
    // LoadIncrementalHfiles will actually do this if none is provided, but because we call it
    // repeatedly in a short amount of time, this seems to trigger a possible race condition
    // where we ask to load the next HFile while there is a pending token cancellation request.
    // By requesting the token ourselves, it is re-used for each bulk load call.
    // Once we're done with the bulk loader we cancel the token.
    if (UserGroupInformation.isSecurityEnabled() && hFileScheme.equals(HDFS_SCHEME)) {
      final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      final DistributedFileSystem fileSystem =
          (DistributedFileSystem) hfilePath.getFileSystem(mConf);
      hdfsDelegationToken = fileSystem.getDelegationToken(RENEWER);
      ugi.addToken(hdfsDelegationToken);
    }

    try {
      // LoadIncrementalHFiles.doBulkLoad() requires an HTable instance, not an HTableInterface:
      final HTable htable = (HTable) mHTableFactory.create(mConf, mHBaseTableName);
      try {
        final List<Path> hfilePaths = Lists.newArrayList();

        // Try to find any hfiles for partitions within the passed in path
        final FileStatus[] hfiles =
            hfilePath.getFileSystem(mConf).globStatus(new Path(hfilePath, "*"));
        for (FileStatus hfile : hfiles) {
          String partName = hfile.getPath().getName();
          if (!partName.startsWith("_") && partName.endsWith(".hfile")) {
            Path partHFile = new Path(hfilePath, partName);
            hfilePaths.add(partHFile);
          }
        }
        if (hfilePaths.isEmpty()) {
          // If we didn't find any parts, add in the passed in parameter
          hfilePaths.add(hfilePath);
        }
        for (Path path : hfilePaths) {
          loader.doBulkLoad(path, htable);
          LOG.info("Successfully loaded: " + path.toString());
        }
      } finally {
        htable.close();
      }
    } catch (TableNotFoundException tnfe) {
      throw new InternalFijiError(tnfe);
    }

    // Cancel the HDFS delegation token if we requested one.
    if (null != hdfsDelegationToken) {
      try {
        hdfsDelegationToken.cancel(mConf);
      } catch (InterruptedException e) {
        LOG.warn("Failed to cancel HDFS delegation token.", e);
      }
    }
  }
}
