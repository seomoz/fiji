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

package com.moz.fiji.mapreduce.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiBufferedWriter;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;

/**
 * Fiji context that writes cells to a configured output table.
 *
 * <p> Implemented as direct writes sent to the HTable.
 *
 * <p> Using this table writer context in a MapReduce is strongly discouraged :
 * pushing a lot of data into a running HBase instance may trigger region splits
 * and cause the HBase instance to go offline.
 */
@ApiAudience.Private
public final class DirectFijiTableWriterContext
    extends InternalFijiContext
    implements FijiTableContext {

  private final Fiji mFiji;
  private final FijiTable mTable;
  private final FijiBufferedWriter mWriter;
  private final EntityIdFactory mEntityIdFactory;

  /**
   * Constructs a new context that can write cells directly to a Fiji table.
   *
   * @param hadoopContext is the Hadoop {@link TaskInputOutputContext} that will be used to perform
   *     the writes.
   * @throws IOException on I/O error.
   */
  public DirectFijiTableWriterContext(TaskInputOutputContext<?, ?, ?, ?> hadoopContext)
      throws IOException {
    super(hadoopContext);
    final Configuration conf = new Configuration(hadoopContext.getConfiguration());
    final FijiURI outputURI =
        FijiURI.newBuilder(conf.get(FijiConfKeys.FIJI_OUTPUT_TABLE_URI)).build();
    mFiji = Fiji.Factory.open(outputURI, conf);
    mTable = mFiji.openTable(outputURI.getTable());
    mWriter = mTable.getWriterFactory().openBufferedWriter();
    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());
  }

  /**
   * Creates a new context that can write cells directly to a Fiji table.
   *
   * @param hadoopContext is the Hadoop {@link TaskInputOutputContext} that will be used to perform
   *     the writes.
   * @return a new context that can write cells directly to a Fiji table.
   * @throws IOException if there is an I/O error.
   */
  public static DirectFijiTableWriterContext
      create(TaskInputOutputContext<?, ?, ?, ?> hadoopContext) throws IOException {
    return new DirectFijiTableWriterContext(hadoopContext);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    mWriter.put(entityId, family, qualifier, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    mWriter.put(entityId, family, qualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    mWriter.deleteRow(entityId);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    mWriter.deleteRow(entityId, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    mWriter.deleteFamily(entityId, family);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    mWriter.deleteFamily(entityId, family, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    mWriter.deleteColumn(entityId, family, qualifier);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    mWriter.deleteColumn(entityId, family, qualifier, upToTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    mWriter.deleteCell(entityId, family, qualifier);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    mWriter.deleteCell(entityId, family, qualifier, timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public EntityIdFactory getEntityIdFactory() {
    return mEntityIdFactory;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... components) {
    return mEntityIdFactory.getEntityId(components);
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    mWriter.flush();
    super.flush();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mWriter.close();
    mTable.release();
    mFiji.release();
    super.close();
  }
}
