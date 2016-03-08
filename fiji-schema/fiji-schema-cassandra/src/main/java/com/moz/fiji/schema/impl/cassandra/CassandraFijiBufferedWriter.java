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
import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiBufferedWriter;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.cassandra.CassandraColumnName;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.impl.DefaultFijiCellEncoderFactory;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.layout.impl.CellEncoderProvider;

/**
 * Cassandra implementation of a batch FijiTableWriter.
 *
 * For now, this implementation is less featured than the HBaseFijiBufferedWriter.  We choose when
 * to execute a series of writes not when the buffer reaches a certain size in raw bytes, but
 * whether when it reaches a certain size in the total number of puts (INSERT statements).
 * We also do not combine puts to the same entity ID together into a single put.
 *
 * We arbitrarily choose to flush the write buffer when it contains 100 statements.
 *
 * We flush the write buffer not by performing a Cassandra batch statement (which is really
 * intended to facilitate writing to multiple tables at once) but by issues all of the writes in
 * parallel using the Cassandra driver's async API and then waiting for all of them to complete.
 *
 * See http://tinyurl.com/pwu2dso for more information about batch statements in Cassandra.
 *
 */
@ApiAudience.Private
@Inheritance.Sealed
@ThreadSafe
public class CassandraFijiBufferedWriter implements FijiBufferedWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiBufferedWriter.class);

  /** FijiTable this writer is attached to. */
  private final CassandraFijiTable mTable;

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** States of a buffered writer instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Monitor against which all internal state mutations must be synchronized. */
  private final Object mMonitor = new Object();

  /** Tracks the state of this buffered writer. */
  @GuardedBy("mMonitor")
  private State mState = State.UNINITIALIZED;

  /** Internal state which must be updated upon table layout change. */
  @GuardedBy("mMonitor")
  private WriterLayoutCapsule mCapsule = null;

  @GuardedBy("mMonitor")
  private long mMaxWriteBufferSize = 100;

  @GuardedBy("mMonitor")
  private long mCurrentWriteBufferSize = 0;

  /** Local write buffers. */
  @GuardedBy("mMonitor")
  private final ListMultimap<CassandraTableName, Statement> mBufferedStatements;

  /**
   * A capsule for writer state which is specific to a table layout version.
   */
  @Immutable
  public static final class WriterLayoutCapsule {
    private final CellEncoderProvider mCellEncoderProvider;
    private final FijiTableLayout mLayout;
    private final CassandraColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellEncoderProvider the encoder provider to store in this capsule.
     * @param layout the table layout to store in this capsule.
     * @param translator the column name translator to store in this capsule.
     */
    public WriterLayoutCapsule(
        final CellEncoderProvider cellEncoderProvider,
        final FijiTableLayout layout,
        final CassandraColumnNameTranslator translator
    ) {
      mCellEncoderProvider = cellEncoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the Cassandra column name translator from the capsule.
     *
     * @return the Cassandra column name translator from this capsule.
     */
    public CassandraColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the table layout from this capsule.
     *
     * @return the table layout from this capsule.
     */
    public FijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * Get the cell encoder provider from this capsule.
     *
     * @return the encoder provider from this capsule.
     */
    public CellEncoderProvider getCellEncoderProvider() {
      return mCellEncoderProvider;
    }
  }

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {

    /** {@inheritDoc} */
    @Override
    public void update(final FijiTableLayout layout) throws IOException {
      synchronized (mMonitor) {
        if (mState == State.CLOSED) {
          LOG.debug("FijiBufferedWriter instance is closed; ignoring layout update.");
          return;
        }
        final FijiURI tableURI = mTable.getURI();
        if (mState == State.OPEN) {
          LOG.info("Flushing buffer for table {} in preparation of layout update.", tableURI);
          flush();
        }

        final CellEncoderProvider provider = new CellEncoderProvider(
            tableURI,
            layout,
            mTable.getFiji().getSchemaTable(),
            DefaultFijiCellEncoderFactory.get());

        if (mCapsule != null) {
          LOG.debug(
              "Updating table writer layout capsule for table '{}' from layout version {} to {}.",
              tableURI,
              mCapsule.getLayout().getDesc().getLayoutId(),
              layout.getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing table writer layout capsule for table '{}' with layout version {}.",
              tableURI,
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
   * Creates a buffered fiji table writer that stores modifications to be sent on command
   * or when the buffer is full.
   *
   * @param table A fiji table.
   * @throws com.moz.fiji.schema.FijiTableNotFoundException in case of an invalid table parameter
   * @throws java.io.IOException in case of IO errors.
   */
  public CassandraFijiBufferedWriter(final CassandraFijiTable table) throws IOException {
    mTable = table;
    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
    Preconditions.checkState(
        mCapsule != null,
        "CassandraFijiBufferedWriter for table: %s failed to initialize.", mTable.getURI());

    mBufferedStatements = ArrayListMultimap.create(
        mTable.getLayout().getLocalityGroups().size(),
        (int) mMaxWriteBufferSize);

    // Retain the table only after everything else succeeded:
    mTable.retain();

    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.UNINITIALIZED,
          "Cannot open CassandraFijiBufferedWriter instance in state %s.", mState);
      mState = State.OPEN;
    }
    ResourceTracker.get().registerResource(this);
  }

  // -----------------------------------------------------------------------------------------------
  // Puts
  // -----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final T value
  ) throws IOException {
    put(entityId, family, qualifier, System.currentTimeMillis(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long timestamp, T value
  ) throws IOException {
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot write to BufferedWriter instance in state %s.", mState);
      final FijiURI tableURI = mTable.getURI();

      final FamilyLayout familyLayout = mCapsule.getLayout().getFamilyMap().get(family);
      if (familyLayout == null) {
        throw new IllegalArgumentException(
            String.format("Unknown family '%s' in table %s.", family, tableURI));
      }

      final CassandraTableName table =
          CassandraTableName.getLocalityGroupTableName(
              tableURI,
              familyLayout.getLocalityGroup().getId());

      // In Cassandra Fiji, a write to HConstants.LATEST_TIMESTAMP should be a write with the
      // current system time.
      final long version;
      if (timestamp == HConstants.LATEST_TIMESTAMP) {
        version = System.currentTimeMillis();
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
              table,
              entityId,
              cassandraColumn,
              version,
              valueBuffer,
              ttl);

      mBufferedStatements.put(table, put);
      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Deletes
  // -----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void deleteRow(final EntityId entityId) throws IOException {
    synchronized (mMonitor) {
      final FijiTableLayout layout = mCapsule.getLayout();
      final CassandraFijiTable tableURI = mTable;
      for (LocalityGroupLayout localityGroup : layout.getLocalityGroups()) {
        final CassandraTableName table =
            CassandraTableName.getLocalityGroupTableName(tableURI.getURI(), localityGroup.getId());
        final Statement delete =
            mTable.getStatementCache().createLocalityGroupDeleteStatement(table, entityId);

        mBufferedStatements.put(table, delete);
        mCurrentWriteBufferSize += 1;
      }

      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(final EntityId entityId, final long upToTimestamp) throws IOException {
    throw new UnsupportedOperationException(
        "Cassandra Fiji does not support deleting a row up-to a timestamp.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(final EntityId entityId, final String family) throws IOException {
    deleteColumn(entityId, family, null);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(
      final EntityId entityId,
      final String family,
      long upToTimestamp
  ) throws IOException {
    throw new UnsupportedOperationException(
        "Cassandra Fiji does not support deleting a family up-to a timestamp.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    final FijiURI tableURI = mTable.getURI();
    synchronized (mMonitor) {
      final FijiTableLayout layout = mCapsule.getLayout();
      final FamilyLayout familyLayout = layout.getFamilyMap().get(family);
      if (familyLayout == null) {
        throw new IllegalArgumentException(
            String.format("Unknown family '%s' in table %s.", family, tableURI));
      }

      final CassandraTableName table =
          CassandraTableName.getLocalityGroupTableName(
              tableURI,
              familyLayout.getLocalityGroup().getId());

      final CassandraColumnName column =
          mCapsule
              .getColumnNameTranslator()
              .toCassandraColumnName(FijiColumnName.create(family, qualifier));


      final Statement delete =
          mTable.getStatementCache().createColumnDeleteStatement(table, entityId, column);
      mBufferedStatements.put(table, delete);

      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long upToTimestamp
  ) throws IOException {
    // TODO(dan): we should be able to support this.
    throw new UnsupportedOperationException(
        "Cassandra Fiji does not support deleting a column up-to a timestamp.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    throw new UnsupportedOperationException(
        "Cassandra Fiji does not support deleting the most-recent version of a cell.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long version
  ) throws IOException {

    final FijiURI tableURI = mTable.getURI();
    synchronized (mMonitor) {
      final FijiTableLayout layout = mCapsule.getLayout();
      final FamilyLayout familyLayout = layout.getFamilyMap().get(family);
      if (familyLayout == null) {
        throw new IllegalArgumentException(
            String.format("Unknown family '%s' in table %s.", family, tableURI));
      }

      final CassandraColumnName column =
          mCapsule
              .getColumnNameTranslator()
              .toCassandraColumnName(FijiColumnName.create(family, qualifier));

      final CassandraTableName table =
          CassandraTableName.getLocalityGroupTableName(
              tableURI,
              familyLayout.getLocalityGroup().getId());

      final Statement delete =
          mTable.getStatementCache().createCellDeleteStatement(table, entityId, column, version);
      mBufferedStatements.put(table, delete);

      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setBufferSize(long bufferSize) throws IOException {
    synchronized (mMonitor) {
      Preconditions.checkState(mState == State.OPEN,
          "Can not set buffer size of BufferedWriter %s in state %s.", this, mState);
      Preconditions.checkArgument(bufferSize >= 0,
          "Buffer size cannot be negative, got %s.", bufferSize);
      mMaxWriteBufferSize = bufferSize;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {

    final List<ResultSetFuture> futures =
        Lists.newArrayList();

    synchronized (mMonitor) {
      LOG.debug("Flushing CassandraFijiBufferedWriter with {} buffered statements.",
          mCurrentWriteBufferSize);

      Preconditions.checkState(mState == State.OPEN,
          "Can not flush BufferedWriter instance %s in state %s.", this, mState);

      // Batch statements in C* are not recommended for performance: see http://tinyurl.com/pwu2dso
      // Just execute all of the write asynchronously instead.
      for (final CassandraTableName table : mBufferedStatements.keySet()) {
        final List<Statement> statements = mBufferedStatements.removeAll(table);

        for (Statement statement : statements) {
          futures.add(mTable.getAdmin().executeAsync(statement));
        }
      }
      mCurrentWriteBufferSize = 0L;
    }

    for (ResultSetFuture future : futures) {
      future.getUninterruptibly();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (mMonitor) {
      flush();
      Preconditions.checkState(mState == State.OPEN,
          "Cannot close BufferedWriter instance %s in state %s.", this, mState);
      mState = State.CLOSED;
      mLayoutConsumerRegistration.close();
      mTable.release();
      ResourceTracker.get().unregisterResource(this);
    }
  }
}
