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
import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.AtomicFijiPutter;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.cassandra.CassandraColumnName;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.impl.DefaultFijiCellEncoderFactory;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiBufferedWriter.WriterLayoutCapsule;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.layout.LayoutUpdatedException;
import com.moz.fiji.schema.layout.impl.CellEncoderProvider;
import com.moz.fiji.schema.layout.impl.ColumnId;

/**
 * Cassandra implementation of AtomicFijiPutter.
 *
 * Facilitates guaranteed atomic puts in batch on a single row to a single locality group.
 *
 * Use {@link #begin} to start a new transaction, {@link #put} to stage a put in the transaction,
 * and {@link #commit} to write all staged puts atomically.
 */
@ApiAudience.Private
@Inheritance.Sealed
@ThreadSafe
public final class CassandraAtomicFijiPutter implements AtomicFijiPutter {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraAtomicFijiPutter.class);

  /** The Fiji table instance. */
  private final CassandraFijiTable mTable;

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** States of an atomic fiji putter instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Monitor against which all internal state mutations must be synchronized. */
  private final Object mMonitor = new Object();

  /** Tracks the state of this atomic fiji putter. */
  @GuardedBy("mMonitor")
  private State mState = State.UNINITIALIZED;

  /** Internal state which must be updated upon table layout change. */
  @GuardedBy("mMonitor")
  private WriterLayoutCapsule mCapsule = null;

  /** EntityId of the row to mutate atomically. */
  @GuardedBy("mMonitor")
  private EntityId mEntityId;

  /** Table name of the current transaction's locality group. */
  @GuardedBy("mMonitor")
  private CassandraTableName mTableName;

  /** Timestamp to use for puts without an explicit timestamps. Set to transaction start time. */
  @GuardedBy("mMonitor")
  private long mTimestamp;

  /** List of statements to execute. */
  @GuardedBy("mMonitor")
  private ArrayList<Statement> mStatements = null;

  /**
   * Set to true when the table calls {@link InnerLayoutUpdater#update(FijiTableLayout)} to
   * indicate a table layout update. Set to false when a user calls {@link #begin(EntityId)}. If
   * this becomes true while a transaction is in progress all methods which would advance the
   * transaction will instead call {@link #rollback()} and throw a {@link LayoutUpdatedException}.
   */
  @GuardedBy("mMonitor")
  private boolean mLayoutChanged = true;

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final FijiTableLayout layout) throws IOException {
      synchronized (mMonitor) {
        final State state = mState;
        Preconditions.checkState(state != State.CLOSED,
            "Cannot update an AtomicFijiPutter instance in state %s.", state);
        mLayoutChanged = true;
        // Update the state of the writer.
        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            layout,
            mTable.getFiji().getSchemaTable(),
            DefaultFijiCellEncoderFactory.get());
        // If the layout is null this is the initial setup and we do not need a log message.
        if (mCapsule != null) {
          LOG.debug(
              "Updating layout used by AtomicFijiPutter: {} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mCapsule.getLayout().getDesc().getLayoutId(),
              layout.getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing AtomicFijiPutter: {} for table: {} with table layout version: {}",
              this,
              mTable.getURI(),
              layout.getDesc().getLayoutId());
        }
        mCapsule = new WriterLayoutCapsule(
            provider,
            layout,
            CassandraColumnNameTranslator.from(layout));
      }
    }
  }

  /**
   * Constructor for this AtomicFijiPutter.
   *
   * @param table The CassandraFijiTable to which this writer writes.
   * @throws java.io.IOException in case of an error.
   */
  public CassandraAtomicFijiPutter(CassandraFijiTable table) throws IOException {
    mTable = table;
    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(
        mCapsule != null,
        "AtomicFijiPutter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    table.retain();
    synchronized (mMonitor) {
      mState = State.OPEN;
    }
  }

  /** Resets the current transaction. */
  private void reset() {
    synchronized (mMonitor) {
      mEntityId = null;
      mStatements = null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void begin(EntityId eid) {
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.OPEN,
          "Can not begin a transaction on an AtomicFijiPutter instance in state %s.", mState);
      if (mStatements != null) {
        throw new IllegalStateException(
            String.format(
                "There is already a transaction in progress on row: %s. Call commit(),"
                    + " checkAndCommit(), or rollback() to clear the current transaction.",
                mEntityId.toShellString()));
      }

      mEntityId = eid;
      mStatements = Lists.newArrayList();
      mLayoutChanged = false;
      mTimestamp = System.currentTimeMillis();
    }
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public void commit() throws IOException {
    synchronized (mMonitor) {
      Preconditions.checkState(mStatements != null,
          "commit() must be paired with a call to begin().");

      Preconditions.checkState(mState == State.OPEN,
          "Can not commit a transaction on an AtomicFijiPutter instance in state %s.", mState);
      // We don't actually need the writer layout capsule here, but we want the layout update check.
      getCapsule();

      Preconditions.checkState(mStatements.size() > 0, "No transactions to commit.");

      final Statement statement;
      if (mStatements.size() > 1) {
        statement = new BatchStatement(BatchStatement.Type.UNLOGGED).addAll(mStatements);
      } else {
        statement = mStatements.get(0);
      }

      ResultSet result = mTable.getAdmin().execute(statement);
      LOG.debug("Results from batch commit: {}.", result);

      reset();
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> boolean checkAndCommit(
      final String family,
      final String qualifier,
      final T value
  ) throws IOException {

    /*
      Unfortunately we can not support the Fiji check and put API because it implicitly relies on
      'latest' timestamp semantics.  Cassandra has support for check and put style transactions
      since 2.0.6, but we can not take advantage of them since we can not know what timestamp we
      should check.

      For now, we just warn.
     */

    LOG.warn("Cassandra AtomicFijiPutter does not support check and commit - just committing "
        + "without checking.");
    commit();
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public void rollback() {
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot rollback a transaction on an AtomicFijiPutter instance in state %s.", mState);
      Preconditions.checkState(mStatements != null,
          "rollback() must be paired with a call to begin()");

      reset();
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final String family,
      final String qualifier,
      final T value
  ) throws IOException {
    synchronized (mMonitor) {
      put(family, qualifier, mTimestamp, value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final String family,
      final String qualifier,
      final long timestamp,
      final T value
  ) throws IOException {
    synchronized (mMonitor) {
      Preconditions.checkState(mStatements != null,
          "Calls to put() must be between calls to begin() "
              + "and commit(), checkAndCommit(), or rollback().");

      Preconditions.checkState(mState == State.OPEN,
          "Can not put cell to an AtomicFijiPutter instance in state %s.", mState);

      final FijiURI tableURI = mTable.getURI();

      final FamilyLayout familyLayout = mCapsule.getLayout().getFamilyMap().get(family);
      if (familyLayout == null) {
        throw new IllegalArgumentException(
            String.format("Unknown family '%s' in table %s.", family, tableURI));
      }

      final ColumnId localityGroupId = familyLayout.getLocalityGroup().getId();
      if (mTableName == null) {
        // first put in transaction; set the table.
        mTableName = CassandraTableName.getLocalityGroupTableName(
            tableURI, familyLayout.getLocalityGroup().getId());
      } else {
        Preconditions.checkArgument(mTableName.getLocalityGroupId().equals(localityGroupId),
            "Fiji Cassandra does not support transactions across multiple locality groups.");
      }

      // In Cassandra Fiji, a write to HConstants.LATEST_TIMESTAMP should be a write with the
      // current transaction time.
      final long version;
      if (timestamp == HConstants.LATEST_TIMESTAMP) {
        version = mTimestamp;
      } else {
        version = timestamp;
      }

      int ttl = familyLayout.getLocalityGroup().getDesc().getTtlSeconds();

      final FijiColumnName columnName = FijiColumnName.create(family, qualifier);
      final CassandraColumnName cassandraColumn =
          mCapsule.getColumnNameTranslator().toCassandraColumnName(columnName);

      final ByteBuffer valueBuffer =
          ByteBuffer.wrap(
              mCapsule.getCellEncoderProvider().getEncoder(family, qualifier).encode(value));

      final Statement put =
          mTable.getStatementCache().createInsertStatment(
              mTableName,
              mEntityId,
              cassandraColumn,
              version,
              valueBuffer,
              ttl);

      mStatements.add(put);
    }
  }

  /**
   * Get the writer layout capsule ensuring that the layout has not been updated while a transaction
   * is in progress.
   *
   * @return the WriterLayoutCapsule for this writer.
   * @throws com.moz.fiji.schema.layout.LayoutUpdatedException in case the table layout has been
   * updated while a transaction is in progress
   */
  private WriterLayoutCapsule getCapsule() throws LayoutUpdatedException {
    synchronized (mMonitor) {
      if (mLayoutChanged) {
        // If the layout was updated, roll back the transaction and throw an Exception to indicate
        // the need to retry.
        rollback();
        // TODO: SCHEMA-468 improve error message for LayoutUpdatedException.
        throw new LayoutUpdatedException(
            "Table layout was updated during a transaction, please retry.");
      } else {
        return mCapsule;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot close an AtomicFijiPutter instance in state %s.", mState);

      if (mStatements != null) {
        LOG.warn("Closing HBaseAtomicFijiPutter while a transaction on table {} on entity {} is "
                + "in progress. Rolling back transaction.", mTable.getURI(), mEntityId);
        reset();
      }

      mLayoutConsumerRegistration.close();
      mTable.release();
      mState = State.CLOSED;
    }
  }
}
