/**
 * (c) Copyright 2014 WibiData, Inc.
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

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiTableWriter;

/**
 * Makes modifications to a Fiji table by sending requests directly to Cassandra from the local
 * client.
 *
 */
@ApiAudience.Private
@ThreadSafe
public final class CassandraFijiTableWriter implements FijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiTableWriter.class);

  private final CassandraFijiBufferedWriter mBufferedWriter;

  /** The fiji table instance. */
  private final CassandraFijiTable mTable;

  /**
   * Creates a non-buffered fiji table writer that sends modifications directly to Fiji.
   *
   * @param table A fiji table.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraFijiTableWriter(CassandraFijiTable table) throws IOException {
    mTable = table;
    mBufferedWriter = table.getWriterFactory().openBufferedWriter();

    // Flush immediately
    mBufferedWriter.setBufferSize(0);

    // Retain the table only when everything succeeds.
    mTable.retain();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final T value
  ) throws IOException {
    mBufferedWriter.put(entityId, family, qualifier, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long timestamp,
      final T value
  ) throws IOException {
    mBufferedWriter.put(entityId, family, qualifier, timestamp, value);
  }

  // ----------------------------------------------------------------------------------------------
  // Counter set, get, increment.

  /** {@inheritDoc} */
  @Override
  public FijiCell<Long> increment(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long amount
  ) throws IOException {
    throw new UnsupportedOperationException("Cassandra Fiji does not support counter columns.");
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public void deleteRow(final EntityId entityId) throws IOException {
    mBufferedWriter.deleteRow(entityId);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(final EntityId entityId, final long upToTimestamp) throws IOException {
    mBufferedWriter.deleteRow(entityId, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(final EntityId entityId, final String family) throws IOException {
    mBufferedWriter.deleteFamily(entityId, family);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(
      final EntityId entityId,
      final String family,
      final long upToTimestamp
  ) throws IOException {
    mBufferedWriter.deleteFamily(entityId, family, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    mBufferedWriter.deleteColumn(entityId, family, qualifier);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long upToTimestamp
  ) throws IOException {
    mBufferedWriter.deleteColumn(entityId, family, qualifier, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    mBufferedWriter.deleteCell(entityId, family, qualifier);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long timestamp
  ) throws IOException {
    mBufferedWriter.deleteCell(entityId, family, qualifier, timestamp);
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    LOG.debug("FijiTableWriter does not need to be flushed.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mBufferedWriter.close();
    mTable.release();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraFijiTableWriter.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .toString();
  }
}
