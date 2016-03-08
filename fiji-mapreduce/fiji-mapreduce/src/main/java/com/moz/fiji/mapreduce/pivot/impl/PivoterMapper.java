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

package com.moz.fiji.mapreduce.pivot.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.impl.FijiTableContextFactory;
import com.moz.fiji.mapreduce.impl.FijiTableMapper;
import com.moz.fiji.mapreduce.pivot.FijiPivoter;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * Hadoop mapper that executes a {@link FijiPivoter} over the rows from a Fiji table.
 */
@ApiAudience.Private
public final class PivoterMapper
    extends FijiTableMapper<HFileKeyValue, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(PivoterMapper.class);

  /** Pivoter that processes input rows. */
  private FijiPivoter mPivoter;

  /** Context allowing interactions with MapReduce, KVStores, etc. */
  private FijiTableContext mContext;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * Returns the specification of the data requested by the pivoter to run over the table.
   * @return the specification of the data requested by the pivoter to run over the table.
   */
  public FijiDataRequest getDataRequest() {
    return mPivoter.getDataRequest();
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

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    super.setup(context);

    Preconditions.checkState(null == mContext);
    setConf(context.getConfiguration());

    mContext = FijiTableContextFactory.create(context);
    mPivoter = FijiPivoters.create(getConf());
    mPivoter.setup(mContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(FijiRowData input, Context context)
      throws IOException {
    Preconditions.checkNotNull(mContext);
    mPivoter.produce(input, mContext);
    mContext.incrementCounter(JobHistoryCounters.PIVOTER_ROWS_PROCESSED);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mContext);
    mPivoter.cleanup(mContext);
    mContext.close();
    mContext = null;

    super.cleanup(context);
  }

}
