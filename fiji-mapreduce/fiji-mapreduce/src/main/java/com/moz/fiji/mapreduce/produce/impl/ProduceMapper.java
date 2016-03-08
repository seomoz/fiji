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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.JobConfigurationException;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.impl.FijiTableMapper;
import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * Hadoop mapper that runs a Fiji producer.
 */
@ApiAudience.Private
public final class ProduceMapper extends FijiTableMapper<HFileKeyValue, NullWritable> {
  /** Actual producer implementation. */
  private FijiProducer mProducer;

  /** Configured output column: either a map-type family or a single column. */
  private FijiColumnName mOutputColumn;

  /** Producer context. */
  private InternalProducerContext mProducerContext;

  /**
   * Return a FijiDataRequest that describes which input columns need to be available.
   *
   * @return A fiji data request.
   */
  public FijiDataRequest getDataRequest() {
    final FijiDataRequest dataRequest = mProducer.getDataRequest();
    if (dataRequest.isEmpty()) {
      throw new JobConfigurationException(mProducer.getClass().getName()
          + " returned an empty FijiDataRequest, which is not allowed.");
    }
    return dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    super.setup(context);
    Preconditions.checkState(mProducerContext == null);
    final Configuration conf = context.getConfiguration();
    mProducer = FijiProducers.create(conf);

    final String column = Preconditions.checkNotNull(mProducer.getOutputColumn());
    mOutputColumn = new FijiColumnName(column);

    mProducerContext = InternalProducerContext.create(context, mOutputColumn);
    mProducer.setup(mProducerContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(FijiRowData input, Context mapContext) throws IOException {
    mProducerContext.setEntityId(input.getEntityId());
    mProducer.produce(input, mProducerContext);
    mapContext.getCounter(JobHistoryCounters.PRODUCER_ROWS_PROCESSED).increment(1);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkState(mProducerContext != null);
    mProducer.cleanup(mProducerContext);
    mProducerContext.close();
    mProducerContext = null;
    super.cleanup(context);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return HFileKeyValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}
