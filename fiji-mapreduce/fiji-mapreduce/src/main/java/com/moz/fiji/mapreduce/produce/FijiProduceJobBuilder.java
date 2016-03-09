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

package com.moz.fiji.mapreduce.produce;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.FijiMultithreadedMapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.JobConfigurationException;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.FijiMapper;
import com.moz.fiji.mapreduce.FijiReducer;
import com.moz.fiji.mapreduce.MapReduceJobOutput;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.framework.FijiTableInputJobBuilder;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.output.FijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.produce.impl.FijiProducers;
import com.moz.fiji.mapreduce.produce.impl.ProduceMapper;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;

/** Builds jobs that run a producer over a Fiji table. */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiProduceJobBuilder extends FijiTableInputJobBuilder<FijiProduceJobBuilder> {

  /** The default number of threads per mapper to use for running producers. */
  private static final int DEFAULT_NUM_THREADS_PER_MAPPER = 1;

  /** The class of the producer to run. */
  private Class<? extends FijiProducer> mProducerClass;

  /** The number of threads per mapper to use for running producers. */
  private int mNumThreadsPerMapper;

  /** Producer job output. */
  private FijiTableMapReduceJobOutput mJobOutput;

  /** The producer instance. */
  private FijiProducer mProducer;

  /** The mapper instance. */
  private FijiMapper<?, ?, ?, ?> mMapper;

  /** The reducer instance. */
  private FijiReducer<?, ?, ?, ?> mReducer;

  /** The data request for the job's table input. */
  private FijiDataRequest mDataRequest;

  /** Constructs a builder for jobs that run a Fiji producer over a Fiji table. */
  private FijiProduceJobBuilder() {
    mProducerClass = null;
    mNumThreadsPerMapper = DEFAULT_NUM_THREADS_PER_MAPPER;
    mJobOutput = null;
    mProducer = null;
    mMapper = null;
    mReducer = null;
    mDataRequest = null;
  }

  /**
   * Creates a new builder for Fiji produce jobs.
   *
   * @return a new Fiji produce job builder.
   */
  public static FijiProduceJobBuilder create() {
    return new FijiProduceJobBuilder();
  }

  /**
   * Configures the job with the Fiji producer to run.
   *
   * @param producerClass The producer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public FijiProduceJobBuilder withProducer(Class<? extends FijiProducer> producerClass) {
    mProducerClass = producerClass;
    return this;
  }

  /**
   * Configures the producer output.
   *
   * @param jobOutput Output table of the producer must match the input table.
   * @return this builder instance so you may chain configuration method calls.
   */
  public FijiProduceJobBuilder withOutput(FijiTableMapReduceJobOutput jobOutput) {
    mJobOutput = jobOutput;
    return super.withOutput(jobOutput);
  }

  /**
   * {@inheritDoc}
   *
   * @param jobOutput Output table of the producer must match the input table. Must be an instance
   *     of FijiTableMapReduceJobOutput or a subclass.
   */
  @Override
  public FijiProduceJobBuilder withOutput(MapReduceJobOutput jobOutput) {
    if (jobOutput instanceof FijiTableMapReduceJobOutput) {
      return withOutput((FijiTableMapReduceJobOutput) jobOutput);
    } else {
      // Throw a more helpful debugging message.
      throw new RuntimeException("jobOutput parameter of FijiProduceJobBuilder.withOutput() must "
          + "be a FijiTableMapReduceJobOutput.");
    }
  }

  /**
   * Sets the number of threads to use for running the producer in parallel.
   *
   * <p>You may use this setting to run multiple instances of your producer in parallel
   * within each map task of the job.  This may useful for increasing throughput when your
   * producer is not CPU bound.</p>
   *
   * @param numThreads The number of produce-runner threads to use per mapper.
   * @return This build instance so you may chain configuration method calls.
   */
  public FijiProduceJobBuilder withNumThreads(int numThreads) {
    Preconditions.checkArgument(numThreads >= 1, "numThreads must be positive, got %d", numThreads);
    mNumThreadsPerMapper = numThreads;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    final Configuration conf = job.getConfiguration();

    // Construct the producer instance.
    if (null == mProducerClass) {
      throw new JobConfigurationException("Must specify a producer.");
    }

    // Serialize the producer class name into the job configuration.
    conf.setClass(FijiConfKeys.FIJI_PRODUCER_CLASS, mProducerClass, FijiProducer.class);

    // Write to the table, but make sure the output table is the same as the input table.
    if (!getInputTableURI().equals(mJobOutput.getOutputTableURI())) {
      throw new JobConfigurationException("Output table must be the same as the input table.");
    }

    // Producers should output to HFiles.
    mMapper = new ProduceMapper();
    mReducer = new IdentityReducer<Object, Object>();

    job.setJobName("Fiji produce: " + mProducerClass.getSimpleName());

    mProducer = ReflectionUtils.newInstance(mProducerClass, job.getConfiguration());
    mDataRequest = mProducer.getDataRequest();

    // Configure the table input job.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected void configureMapper(Job job) throws IOException {
    super.configureMapper(job);

    // Configure map-parallelism if configured.
    if (mNumThreadsPerMapper > 1) {
      @SuppressWarnings("unchecked")
      Class<? extends Mapper<EntityId, FijiRowData, Object, Object>> childMapperClass
          = (Class<? extends Mapper<EntityId, FijiRowData, Object, Object>>) mMapper.getClass();
      FijiMultithreadedMapper.setMapperClass(job, childMapperClass);
      FijiMultithreadedMapper.setNumberOfThreads(job, mNumThreadsPerMapper);
      job.setMapperClass(FijiMultithreadedMapper.class);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mProducer.getRequiredStores();
  }

  /** {@inheritDoc} */
  @Override
  protected FijiMapReduceJob build(Job job) {
    return FijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected FijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected void validateInputTable(FijiTable inputTable) throws IOException {
    // Validate the Fiji data request against the input table layout:
    super.validateInputTable(inputTable);

    // Validate the producer output column against the output table (ie. the input table):
    FijiProducers.validateOutputColumn(mProducer, inputTable.getLayout());
  }

  /** {@inheritDoc} */
  @Override
  protected FijiMapper<?, ?, ?, ?> getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected FijiReducer<?, ?, ?, ?> getCombiner() {
    // Producers can't have combiners.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  protected FijiReducer<?, ?, ?, ?> getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mProducerClass;
  }
}
