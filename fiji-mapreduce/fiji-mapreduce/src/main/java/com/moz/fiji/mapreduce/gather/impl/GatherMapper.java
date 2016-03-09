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

package com.moz.fiji.mapreduce.gather.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.avro.AvroKeyWriter;
import com.moz.fiji.mapreduce.avro.AvroValueWriter;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.gather.GathererContext;
import com.moz.fiji.mapreduce.gather.FijiGatherer;
import com.moz.fiji.mapreduce.impl.FijiTableMapper;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * Mapper that executes a gatherer over the rows of a Fiji table.
 *
 * @param <K> The type of the MapReduce output key.
 * @param <V> The type of the MapReduce output value.
 */
@ApiAudience.Private
public final class GatherMapper<K, V>
    extends FijiTableMapper<K, V>
    implements AvroKeyWriter, AvroValueWriter {

  private static final Logger LOG = LoggerFactory.getLogger(GatherMapper.class);

  /** The gatherer to execute. */
  private FijiGatherer<K, V> mGatherer;

  /**
   * The context object that allows the gatherer to interact with MapReduce,
   * KVStores, etc.
   */
  private GathererContext<K, V> mGathererContext;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    setGatherer(createGatherer(conf));
  }

  /**
   * Return a FijiDataRequest that describes which input columns need to be available.
   *
   * @return A fiji data request.
   */
  public FijiDataRequest getDataRequest() {
    return mGatherer.getDataRequest();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return mGatherer.getOutputKeyClass();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return mGatherer.getOutputValueClass();
  }

  /**
   * Initialize the gatherer instance to execute.
   *
   * @param conf the Configuration to use to initialize the gatherer instance.
   * @return a FijiGatherer instance.
   */
  protected FijiGatherer<K, V> createGatherer(Configuration conf) {
    @SuppressWarnings("unchecked")
    Class<? extends FijiGatherer<K, V>> gatherClass =
        (Class<? extends FijiGatherer<K, V>>)
        conf.getClass(FijiConfKeys.FIJI_GATHERER_CLASS, null, FijiGatherer.class);
    if (null == gatherClass) {
      LOG.error("Null " + FijiConfKeys.FIJI_GATHERER_CLASS + " in createGatherer()?");
      return null;
    }
    return ReflectionUtils.newInstance(gatherClass, conf);
  }

  /**
   * Set the gatherer instance to use for the scan.
   *
   * @param gatherer the gatherer instance to use.
   */
  private void setGatherer(FijiGatherer<K, V> gatherer) {
    mGatherer = gatherer;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    super.setup(context);

    Preconditions.checkState(null == mGathererContext);
    setConf(context.getConfiguration());

    mGathererContext = InternalGathererContext.create(context);
    mGatherer.setup(mGathererContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(FijiRowData input, Context context)
      throws IOException {
    Preconditions.checkNotNull(mGathererContext);
    mGatherer.gather(input, mGathererContext);
    mGathererContext.incrementCounter(JobHistoryCounters.GATHERER_ROWS_PROCESSED);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mGathererContext);
    mGatherer.cleanup(mGathererContext);
    mGathererContext.close();
    mGathererContext = null;

    super.cleanup(context);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    if (mGatherer instanceof AvroKeyWriter) {
      LOG.debug("Gatherer " + mGatherer.getClass().getName()
          + " implements AvroKeyWriter, querying for writer schema.");
      return ((AvroKeyWriter) mGatherer).getAvroKeyWriterSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    if (mGatherer instanceof AvroValueWriter) {
      LOG.debug("Gatherer " + mGatherer.getClass().getName()
          + " implements AvroValueWriter, querying for writer schema.");
      return ((AvroValueWriter) mGatherer).getAvroValueWriterSchema();
    }
    return null;
  }
}
