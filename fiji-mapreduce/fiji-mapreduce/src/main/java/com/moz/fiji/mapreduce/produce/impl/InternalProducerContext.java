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

package com.moz.fiji.mapreduce.produce.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.impl.InternalFijiContext;
import com.moz.fiji.mapreduce.impl.FijiTableContextFactory;
import com.moz.fiji.mapreduce.produce.ProducerContext;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiRowData;

/**
 * Implementation of a producer context.
 *
 * Wraps a full FijiTableContext and restricts it to puts allowed in a producer.
 */
@ApiAudience.Private
public final class InternalProducerContext
    extends InternalFijiContext implements ProducerContext {

  /** Interface to write to the output table. */
  private final FijiTableContext mTableContext;

  /** Family to write to. */
  private final String mFamily;

  /** Qualifier to write to (may be null, when output is a map-type family). */
  private final String mQualifier;

  /** The entity id of the row being written. */
  private EntityId mEntityId;

  /**
   * Initializes a producer context.
   *
   * @param taskContext Underlying Hadoop context.
   * @param outputColumn Column to write.
   * @throws IOException on I/O error.
   */
  private InternalProducerContext(
      TaskInputOutputContext<EntityId, FijiRowData, ?, ?> taskContext,
      FijiColumnName outputColumn)
      throws IOException {
    super(taskContext);
    mTableContext = FijiTableContextFactory.create(taskContext);
    mFamily = Preconditions.checkNotNull(outputColumn.getFamily());
    mQualifier = outputColumn.getQualifier();
  }

  /**
   * Creates a new implementation of {@link InternalProducerContext} for use by Fiji producers.
   *
   * @param taskContext is the Hadoop {@link TaskInputOutputContext} to which the new context's
   *    functionality will be delegated.
   * @param outputColumn is the name of the Fiji column that the new context can write to.
   * @return a new context for use by Fiji producers that can write to a column of a Fiji table.
   * @throws IOException if there is an I/O error.
   */
  public static InternalProducerContext create(
      TaskInputOutputContext<EntityId, FijiRowData, ?, ?> taskContext,
      FijiColumnName outputColumn) throws IOException {
    return new InternalProducerContext(taskContext, outputColumn);
  }

  /**
   * Gets an EntityId as set by {@link #setEntityId(EntityId)} ()}.
   *
   * @return the previously set EntityId.
   */
  public EntityId getEntityId() {
    return mEntityId;
  }

  /**
   * Sets the entity ID of the current row being processed and written to.
   *
   * @param entityId Entity ID of the row being processed and written to.
   * @return this context.
   */
  public InternalProducerContext setEntityId(EntityId entityId) {
    mEntityId = entityId;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(T value)
      throws IOException {
    put(HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String qualifier, T value)
      throws IOException {
    put(qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(long timestamp, T value)
      throws IOException {
    Preconditions.checkNotNull(mEntityId);
    Preconditions.checkNotNull(mQualifier,
        "Producer output configured for a map-type family, use put(qualifier, timestamp, value)");
    mTableContext.put(mEntityId, mFamily, mQualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String qualifier, long timestamp, T value)
      throws IOException {
    Preconditions.checkNotNull(mEntityId);
    Preconditions.checkState(null == mQualifier,
        "Qualifier already specified by producer configuration.");

    mTableContext.put(mEntityId, mFamily, qualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    mTableContext.flush();
    super.flush();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mTableContext.close();
    super.close();
  }
}
