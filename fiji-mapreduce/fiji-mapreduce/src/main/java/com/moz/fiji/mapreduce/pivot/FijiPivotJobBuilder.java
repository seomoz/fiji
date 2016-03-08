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

package com.moz.fiji.mapreduce.pivot;

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
import com.moz.fiji.mapreduce.pivot.impl.PivoterMapper;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * Builds jobs that run a {@link FijiPivoter} over a Fiji table.
 *
 * <p>
 *   {@link FijiPivoter} scans the rows from an input FijiTable and writes cells
 *   into an output FijiTable. The input and the output FijiTable may or may not be the same.
 * </p>
 *
 * <p>
 *   Use the {@link FijiPivotJobBuilder} to configure a {@link FijiPivoter} job, by specifying:
 *   <ul>
 *     <li> the {@link FijiPivoter} class to run over the input FijiTable; </li>
 *     <li> the input {@link com.moz.fiji.schema.FijiTable} to be processed by the {@link FijiPivoter};
 *     </li>
 *     <li> the output {@link com.moz.fiji.schema.FijiTable} the {@link FijiPivoter} writes to. </li>
 *   </ul>
 * </p>
 *
 * <p> Example:
 * <pre><blockquote>
 *   final Configuration conf = ...;
 *   final FijiURI inputTableURI = ...;
 *   final FijiURI outputTableURI = ...;
 *   final FijiMapReduceJob job = FijiPivotJobBuilder.create()
 *       .withConf(conf)
 *       .withPivoter(SomePivoter.class)
 *       .withInputTable(inputTableURI)
 *       .withOutput(MapReduceJobOutputs
 *           .newHFileMapReduceJobOutput(outputTableURI, hfilePath))
 *       .build();
 *   job.run();
 * </blockquote></pre>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class FijiPivotJobBuilder
    extends FijiTableInputJobBuilder<FijiPivotJobBuilder> {

  /** Default number of threads per mapper to use for running pivoters. */
  private static final int DEFAULT_NUM_THREADS_PER_MAPPER = 1;

  /** {@link FijiPivoter} class to run over the table. */
  private Class<? extends FijiPivoter> mPivoterClass;

  /** Configured number of threads per mapper to use for running pivoters. */
  private int mNumThreadsPerMapper;

  /** Pivoter to run for this FijiMR pivot job. */
  private FijiPivoter mPivoter;

  /** Hadoop mapper to run for this FijiMR pivot job. */
  private FijiMapper<?, ?, ?, ?> mMapper;

  /** Hadoop reducer to run for this FijiMR pivot job. */
  private FijiReducer<?, ?, ?, ?> mReducer;

  /** Specification of the data requested for this pivot job. */
  private FijiDataRequest mDataRequest;

  /** Constructs a builder for jobs that run a Fiji table-mapper over a Fiji table. */
  private FijiPivotJobBuilder() {
    mPivoterClass = null;
    mNumThreadsPerMapper = DEFAULT_NUM_THREADS_PER_MAPPER;
    mPivoter = null;
    mMapper = null;
    mReducer = null;
    mDataRequest = null;
  }

  /**
   * Creates a new builder for a {@link FijiPivoter} job.
   *
   * @return a new builder for a {@link FijiPivoter} job.
   */
  public static FijiPivotJobBuilder create() {
    return new FijiPivotJobBuilder();
  }

  /**
   * Configures the job with the {@link FijiPivoter} to run.
   *
   * @param pivoterClass {@link FijiPivoter} class to run over the input Fiji table.
   * @return this builder instance.
   */
  public FijiPivotJobBuilder withPivoter(
      Class<? extends FijiPivoter> pivoterClass
  ) {
    mPivoterClass = pivoterClass;
    return this;
  }

  /**
   * Configures the output table of this pivoter.
   *
   * @param jobOutput Fiji table the pivoter writes to.
   * @return this builder instance.
   */
  public FijiPivotJobBuilder withOutput(FijiTableMapReduceJobOutput jobOutput) {
    return super.withOutput(jobOutput);
  }

  /**
   * {@inheritDoc}
   *
   * <p> The output of a pivoter must be a FijiTable. </p>
   */
  @Override
  public FijiPivotJobBuilder withOutput(MapReduceJobOutput jobOutput) {
    if (jobOutput instanceof FijiTableMapReduceJobOutput) {
      return withOutput((FijiTableMapReduceJobOutput) jobOutput);
    } else {
      throw new RuntimeException("FijiTableRWMapper must output to a Fiji table.");
    }
  }

  /**
   * Sets the number of threads to use for running the producer in parallel.
   *
   * <p>You may use this setting to run multiple instances of the pivoter in parallel
   * within each map task of the job.  This may useful for increasing throughput when the
   * pivoter is not CPU bound.</p>
   *
   * @param numThreads Number of threads to use per mapper.
   * @return this build instance.
   */
  public FijiPivotJobBuilder withNumThreads(int numThreads) {
    Preconditions.checkArgument(numThreads >= 1, "numThreads must be positive, got %d", numThreads);
    mNumThreadsPerMapper = numThreads;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    final Configuration conf = job.getConfiguration();

    if (null == mPivoterClass) {
      throw new JobConfigurationException("Must specify a FijiPivoter class.");
    }

    // Serialize the pivoter class name into the job configuration.
    conf.setClass(FijiConfKeys.KIJI_PIVOTER_CLASS, mPivoterClass, FijiPivoter.class);

    // Producers should output to HFiles.
    mMapper = new PivoterMapper();
    mReducer = new IdentityReducer<Object, Object>();

    job.setJobName("FijiPivoter: " + mPivoterClass.getSimpleName());

    mPivoter = ReflectionUtils.newInstance(mPivoterClass, job.getConfiguration());
    mDataRequest = mPivoter.getDataRequest();

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
      Class<? extends Mapper<EntityId, FijiRowData, Object, Object>> childMapperClass =
          (Class<? extends Mapper<EntityId, FijiRowData, Object, Object>>) mMapper.getClass();
      FijiMultithreadedMapper.setMapperClass(job, childMapperClass);
      FijiMultithreadedMapper.setNumberOfThreads(job, mNumThreadsPerMapper);
      job.setMapperClass(FijiMultithreadedMapper.class);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mPivoter.getRequiredStores();
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
  protected FijiMapper<?, ?, ?, ?> getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected FijiReducer<?, ?, ?, ?> getCombiner() {
    // A pivoter cannot have combiners.
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
    return mPivoterClass;
  }
}
