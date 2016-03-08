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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiReaderFactory;
import com.moz.fiji.schema.FijiRegion;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableAnnotator;
import com.moz.fiji.schema.FijiTableNotFoundException;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.cassandra.CassandraFijiURI;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.impl.LayoutConsumer.Registration;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.TableLayoutMonitor;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.VersionInfo;

/**
 * <p>A FijiTable that exposes the underlying Cassandra implementation.</p>
 *
 * <p>Within the internal Fiji code, we use this class so that we have
 * access to the Cassandra interface.  Methods that Fiji clients should
 * have access to should be added to com.moz.fiji.schema.FijiTable.</p>
 */
@ApiAudience.Private
public final class CassandraFijiTable implements FijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiTable.class);

  /** The fiji instance this table belongs to. */
  private final CassandraFiji mFiji;

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
     * incremented.  Resources are successfully opened and this FijiTable's methods may be
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

  /** CassandraAdmin object that we use for interacting with the open C* session. */
  private final CassandraAdmin mAdmin;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /** Retain counter. When decreased to 0, the FijiTable may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /** Writer factory for this table. */
  private final CassandraFijiWriterFactory mWriterFactory;

  /** Reader factory for this table. */
  private final FijiReaderFactory mReaderFactory;

  /**
   * Monitor for the layout of this table. Should be initialized in the constructor and nulled out
   * in {@link #closeResources()}. No other method should modify this pointer.
   **/
  private volatile TableLayoutMonitor mLayoutMonitor;

  /**
   * A cached version of the table's row key format.  Safe to cache, because row key formats can not
   * be modified after table creation.
   */
  private final RowKeyFormat2 mRowKeyFormat;

  /**
   * Construct an opened Fiji table stored in Cassandra.
   *
   * @param fiji The Fiji instance.
   * @param name The name of the Fiji user-space table to open.
   * @param admin The Cassandra admin object.
   * @param layoutMonitor for the Fiji table.
   * @throws java.io.IOException On a C* error.
   *     <p> Throws FijiTableNotFoundException if the table does not exist. </p>
   */
  CassandraFijiTable(
      final CassandraFiji fiji,
      final String name,
      final CassandraAdmin admin,
      final TableLayoutMonitor layoutMonitor)
      throws IOException {
    mFiji = fiji;
    mFiji.retain();

    mName = name;
    mAdmin = admin;
    mTableURI = CassandraFijiURI.newBuilder(mFiji.getURI()).withTableName(mName).build();
    LOG.debug("Opening Fiji table '{}' with client version '{}'.",
        mTableURI, VersionInfo.getSoftwareVersion());

    if (!mFiji.getTableNames().contains(mName)) {
      closeResources();
      throw new FijiTableNotFoundException(mTableURI);
    }

    mWriterFactory = new CassandraFijiWriterFactory(this);
    mReaderFactory = new CassandraFijiReaderFactory(this);

    mLayoutMonitor = layoutMonitor;
    mRowKeyFormat = (RowKeyFormat2) mLayoutMonitor.getLayout().getDesc().getKeysFormat();
    mEntityIdFactory = EntityIdFactory.getFactory(mRowKeyFormat);

    // Table is now open and must be released properly:
    mRetainCount.set(1);

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open FijiTable instance in state %s.", oldState);
    ResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... fijiRowKey) {
    return mEntityIdFactory.getEntityId(fijiRowKey);
  }

  /**
   * Get the CQL statement cache for this Fiji Cassandra table.
   *
   * @return The CQL statement cache for this Fiji Cassandra table.
   */
  public CQLStatementCache getStatementCache() {
    return mAdmin.getStatementCache(mRowKeyFormat);
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
   * completed a table layout update.  Sends the first update immediately before returning.
   *
   * @param consumer the LayoutConsumer to be registered.
   * @return a registration object which must be closed when layout updates are no longer needed.
   * @throws java.io.IOException in case of an error updating the LayoutConsumer.
   */
  public Registration registerLayoutConsumer(LayoutConsumer consumer) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot register a new layout consumer to a FijiTable in state %s.", state);
    return mLayoutMonitor.registerLayoutConsumer(consumer);
  }

  /**
   * {@inheritDoc}
   *
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should create the column name translator from this layout to ensure consistency.
   */
  @Override
  public FijiTableLayout getLayout() {
    return mLayoutMonitor.getLayout();
  }

  /** {@inheritDoc} */
  @Override
  public CassandraFijiTableReader openTableReader() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table reader on a FijiTable in state %s.", state);
    try {
      return CassandraFijiTableReader.create(this);
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
      return new CassandraFijiTableWriter(this);
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
  public CassandraFijiWriterFactory getWriterFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the writer factory for a FijiTable in state %s.", state);
    return mWriterFactory;
  }

  /**
   * Does not really make sense for a Cassandra implementation.
   *
   * @return An empty list.
   * @throws java.io.IOException on I/O error.
   */
  @Override
  public List<FijiRegion> getRegions() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the regions for a FijiTable in state %s.", state);
    throw new UnsupportedOperationException("Cassandra-backed Fiji tables do not have regions.");
  }

  /** {@inheritDoc} */
  @Override
  public Collection<CassandraFijiPartition> getPartitions() throws IOException {
    return CassandraFijiPartition.getPartitions(mAdmin.getSession());
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableAnnotator openTableAnnotator() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the TableAnnotator for a table in state: %s.", state);
    return new CassandraFijiTableAnnotator(this);
  }

  /**
   * Releases the resources used by this table.
   *
   * @throws java.io.IOException on I/O error.
   */
  private void closeResources() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close FijiTable instance %s in state %s.", this, oldState);
    LOG.debug("Closing CassandraFijiTable '{}'.", this);

    ResourceUtils.releaseOrLog(mFiji);
    if (oldState != State.UNINITIALIZED) {
      ResourceTracker.get().unregisterResource(this);
    }

    // Relinquish strong reference to the TableLayoutMonitor in case the user keeps their reference
    // to this FijiTable.
    mLayoutMonitor = null;

    LOG.debug("CassandraFijiTable '{}' closed.", mTableURI);
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
    return Objects.toStringHelper(CassandraFijiTable.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mTableURI)
        .add("retain_counter", mRetainCount.get())
        .add("layout_id", getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * Getter method for this instances C* admin.
   *
   * Necessary now so that readers and writers can execute CQL commands.
   *
   * @return The C* admin object for this table.
   */
  public CassandraAdmin getAdmin() {
    return mAdmin;
  }
}
