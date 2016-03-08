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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.AtomicFijiPutter;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCellEncoder;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.impl.DefaultFijiCellEncoderFactory;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.impl.hbase.HBaseFijiTableWriter.WriterLayoutCapsule;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.LayoutUpdatedException;
import com.moz.fiji.schema.layout.impl.CellEncoderProvider;
import com.moz.fiji.schema.platform.SchemaPlatformBridge;

/**
 * HBase implementation of AtomicFijiPutter.
 *
 * Access via HBaseFijiWriterFactory.openAtomicFijiPutter(), facilitates guaranteed atomic
 * puts in batch on a single row.
 *
 * Use <code>begin(EntityId)</code> to open a new transaction,
 * <code>put(family, qualifier, value)</code> to stage a put in the transaction,
 * and <code>commit()</code> or <code>checkAndCommit(family, qualifier, value)</code>
 * to write all staged puts atomically.
 *
 * This class is not thread-safe.  It is the user's responsibility to protect against
 * concurrent access to a writer while a transaction is being constructed.
 */
@ApiAudience.Private
public final class HBaseAtomicFijiPutter implements AtomicFijiPutter {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseAtomicFijiPutter.class);

  /** The Fiji table instance. */
  private final HBaseFijiTable mTable;

  /** The HTableInterface associated with the FijiTable. */
  private final HTableInterface mHTable;

  /** States of an atomic fiji putter instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this atomic fiji putter. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Lock for synchronizing layout update mutations. */
  private final Object mLock = new Object();

  /** EntityId of the row to mutate atomically. */
  private EntityId mEntityId;

  /** HBaseRowKey of the row to mutate. */
  private byte[] mId;

  /** List of HBase KeyValue objects to be written. */
  private ArrayList<KeyValue> mHopper = null;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * Composite Put containing batch puts.
   * mPut is null outside of a begin() — commit()/rollback() transaction.
   * mPut is non null inside of a begin() — commit()/rollback() transaction.
   */
  private Put mPut = null;

  /**
   * <p>
   *   Set to true when the table calls {@link InnerLayoutUpdater#update} to indicate a table layout
   *   update.  Set to false when a user calls {@link #begin(com.moz.fiji.schema.EntityId)}.  If this
   *   becomes true while a transaction is in progress all methods which would advance the
   *   transaction will instead call {@link #rollback()} and throw a {@link LayoutUpdatedException}.
   * </p>
   * <p>
   *   Access to this variable must be protected by synchronizing on mLock.
   * </p>
   */
  private boolean mLayoutOutOfDate = false;

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final FijiTableLayout layout) throws IOException {
      if (mState.get() == State.CLOSED) {
        LOG.debug("AtomicFijiPutter instance is closed; ignoring layout update.");
        return;
      }
      synchronized (mLock) {
        mLayoutOutOfDate = true;
        // Update the state of the writer.
        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            layout,
            mTable.getFiji().getSchemaTable(),
            DefaultFijiCellEncoderFactory.get());
        // If the capsule is null this is the initial setup and we do not need a log message.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by AtomicFijiPutter: {} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              layout.getDesc().getLayoutId());
        } else {
          LOG.debug("Initializing AtomicFijiPutter: {} for table: {} with table layout version: {}",
              this,
              mTable.getURI(),
              layout.getDesc().getLayoutId());
        }
        mWriterLayoutCapsule =
            new WriterLayoutCapsule(provider, layout, HBaseColumnNameTranslator.from(layout));
      }
    }
  }

  /**
   * Constructor for this AtomicFijiPutter.
   *
   * @param table The HBaseFijiTable to which this writer writes.
   * @throws IOException in case of an error.
   */
  public HBaseAtomicFijiPutter(HBaseFijiTable table) throws IOException {
    mTable = table;
    mHTable = mTable.openHTableConnection();
    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "AtomicFijiPutter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    table.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open AtomicFijiPutter instance in state %s.", oldState);
  }

  /** Resets the current transaction. */
  private void reset() {
    mPut = null;
    mEntityId = null;
    mHopper = null;
    mId = null;
  }

  /** {@inheritDoc} */
  @Override
  public void begin(EntityId eid) {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot begin a transaction on an AtomicFijiPutter instance in state %s.", state);
    // Preconditions.checkArgument() cannot be used here because mEntityId is null between calls to
    // begin().
    if (mPut != null) {
      throw new IllegalStateException(String.format("There is already a transaction in progress on "
          + "row: %s. Call commit(), checkAndCommit(), or rollback() to clear the Put.",
          mEntityId.toShellString()));
    }
    synchronized (mLock) {
      mLayoutOutOfDate = false;
    }
    mEntityId = eid;
    mId = eid.getHBaseRowKey();
    mHopper = new ArrayList<KeyValue>();
    mPut = new Put(mId);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public void commit() throws IOException {
    Preconditions.checkState(mPut != null, "commit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot commit a transaction on an AtomicFijiPutter instance in state %s.", state);
    // We don't actually need the writer layout capsule here, but we want the layout update check.
    getWriterLayoutCapsule();
    SchemaPlatformBridge bridge = SchemaPlatformBridge.get();
    for (KeyValue kv : mHopper) {
      bridge.addKVToPut(mPut, kv);
    }

    mHTable.put(mPut);
    if (!mHTable.isAutoFlush()) {
      mHTable.flushCommits();
    }
    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException {
    Preconditions.checkState(mPut != null,
        "checkAndCommit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot checkAndCommit a transaction on an AtomicFijiPutter instance in state %s.", state);
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final FijiColumnName fijiColumnName = FijiColumnName.create(family, qualifier);
    final HBaseColumnName columnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(fijiColumnName);
    final byte[] encoded;

    // If passed value is null, then let encoded value be null.
    // HBase will check for non-existence of cell.
    if (null == value) {
      encoded = null;
    } else {
      final FijiCellEncoder cellEncoder =
          capsule.getCellEncoderProvider().getEncoder(family, qualifier);
      encoded = cellEncoder.encode(value);
    }

    SchemaPlatformBridge bridge = SchemaPlatformBridge.get();
    for (KeyValue kv : mHopper) {
      bridge.addKVToPut(mPut, kv);
    }

    boolean retVal = mHTable.checkAndPut(
        mId, columnName.getFamily(), columnName.getQualifier(), encoded, mPut);
    if (retVal) {
      if (!mHTable.isAutoFlush()) {
        mHTable.flushCommits();
      }
      reset();
    }
    return retVal;
  }

  /** {@inheritDoc} */
  @Override
  public void rollback() {
    Preconditions.checkState(mPut != null, "rollback() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot rollback a transaction on an AtomicFijiPutter instance in state %s.", state);
    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, T value) throws IOException {
    put(family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, long timestamp, T value) throws IOException {
    Preconditions.checkState(mPut != null, "calls to put() must be between calls to begin() and "
        + "commit(), checkAndCommit(), or rollback()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to an AtomicFijiPutter instance in state %s.", state);
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final FijiColumnName fijiColumnName = FijiColumnName.create(family, qualifier);
    final HBaseColumnName columnName =
        capsule.getColumnNameTranslator().toHBaseColumnName(fijiColumnName);

    final FijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    mHopper.add(new KeyValue(
        mId, columnName.getFamily(), columnName.getQualifier(), timestamp, encoded));
  }

  /**
   * Get the writer layout capsule ensuring that the layout has not been updated while a transaction
   * is in progress.
   *
   * @return the WriterLayoutCapsule for this writer.
   * @throws LayoutUpdatedException in case the table layout has been updated while a transaction is
   * in progress
   */
  private WriterLayoutCapsule getWriterLayoutCapsule() throws LayoutUpdatedException {
    synchronized (mLock) {
      if (mLayoutOutOfDate) {
        // If the layout was updated, roll back the transaction and throw an Exception to indicate
        // the need to retry.
        rollback();
        // TODO: SCHEMA-468 improve error message for LayoutUpdatedException.
        throw new LayoutUpdatedException(
            "Table layout was updated during a transaction, please retry.");
      } else {
        return mWriterLayoutCapsule;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close an AtomicFijiPutter instance in state %s.", oldState);
    if (mPut != null) {
      LOG.warn("Closing HBaseAtomicFijiPutter while a transaction on table {} on entity ID {} is "
          + "in progress. Rolling back transaction.", mTable.getURI(), mEntityId);
      reset();
    }
    mLayoutConsumerRegistration.close();
    mHTable.close();
    mTable.release();
  }
}
