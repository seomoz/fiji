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

package com.moz.fiji.mapreduce.gather;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.JobConfigurationException;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.FijiMapper;
import com.moz.fiji.mapreduce.FijiReducer;
import com.moz.fiji.mapreduce.MapReduceJobOutput;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.framework.FijiTableInputJobBuilder;
import com.moz.fiji.mapreduce.gather.impl.GatherMapper;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreClient;
import com.moz.fiji.mapreduce.output.HFileMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.framework.HFileReducerMapReduceJobOutput;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;
import com.moz.fiji.schema.FijiDataRequest;

/**
 * Builds jobs that run a gatherer over a Fiji table.
 *
 * <p>Example usage:</p>
 * <pre><code>
 * MapReduceJob job = FijiGatherJobBuilder.create()
 *     .withInputTable(myTable)
 *     .withGatherer(MyCountGatherer.class)
 *     .withReducer(SimpleIntSumReducer.class)
 *     .withOutput(new TextMapReduceJobOutput("path/to/counts", numSplits))
 *     .build();
 * boolean success = job.run();
 * </code></pre>
 */
@SuppressWarnings("rawtypes")
@ApiAudience.Public
@ApiStability.Stable
public final class FijiGatherJobBuilder extends FijiTableInputJobBuilder<FijiGatherJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiGatherJobBuilder.class);

  /** The class of the gatherer to run. */
  private Class<? extends FijiGatherer> mGathererClass;

  /** The class of the combiner to run. */
  private Class<? extends FijiReducer> mCombinerClass;

  /** The class of the reducer to run. */
  private Class<? extends FijiReducer> mReducerClass;

  private GatherMapper mMapper;

  /** The gatherer instance. */
  private FijiGatherer<?, ?> mGatherer;

  /** The combiner instance (may be null if no combiner is specified). */
  private FijiReducer<?, ?, ?, ?> mCombiner;

  /** The reducer instance (may be null if no reducer is specified). */
  private FijiReducer<?, ?, ?, ?> mReducer;

  /** The data request for the job's table input. */
  private FijiDataRequest mDataRequest;

  /** Constructs a builder for jobs that run a Fiji gatherer over a Fiji table. */
  private FijiGatherJobBuilder() {
    mGathererClass = null;
    mCombinerClass = null;
    mReducerClass = null;

    mMapper = new GatherMapper();
    mGatherer = null;
    mCombiner = null;
    mReducer = null;

    mDataRequest = null;
  }

  /**
   * Creates a new builder for Fiji gather jobs.
   *
   * @return a new Fiji gather job builder.
   */
  public static FijiGatherJobBuilder create() {
    return new FijiGatherJobBuilder();
  }

  /**
   * Configures the job with the Fiji gatherer to run in the map phase.
   *
   * @param gathererClass The gatherer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public FijiGatherJobBuilder withGatherer(Class<? extends FijiGatherer> gathererClass) {
    mGathererClass = gathererClass;
    return this;
  }

  /**
   * Configures the job with a combiner to run (optional).
   *
   * @param combinerClass The combiner class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public FijiGatherJobBuilder withCombiner(Class<? extends FijiReducer> combinerClass) {
    mCombinerClass = combinerClass;
    return this;
  }

  /**
   * Configures the job with a reducer to run (optional).
   *
   * @param reducerClass The reducer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public FijiGatherJobBuilder withReducer(Class<? extends FijiReducer> reducerClass) {
    mReducerClass = reducerClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Construct the gatherer instance.
    if (null == mGathererClass) {
      throw new JobConfigurationException("Must specify a gatherer.");
    }

    final Configuration conf = job.getConfiguration();

    // Serialize the gatherer class name into the job configuration.
    conf.setClass(FijiConfKeys.KIJI_GATHERER_CLASS, mGathererClass, FijiGatherer.class);

    if ((getJobOutput() instanceof HFileMapReduceJobOutput) && (null == mReducerClass)) {
      mReducerClass = IdentityReducer.class;
    }

    final StringBuilder name = new StringBuilder("Fiji gather: " + mGathererClass.getSimpleName());
    if (null != mReducerClass) {
      name.append(" / " + mReducerClass.getSimpleName());
    }
    job.setJobName(name.toString());

    mGatherer = ReflectionUtils.newInstance(mGathererClass, conf);
    mMapper.setConf(conf);
    mDataRequest = mGatherer.getDataRequest();

    // Construct the combiner instance (if specified).
    if (null != mCombinerClass) {
      mCombiner = ReflectionUtils.newInstance(mCombinerClass, conf);
    }

    // Construct the reducer instance (if specified).
    if (null != mReducerClass) {
      mReducer = ReflectionUtils.newInstance(mReducerClass, conf);
    }

    // Configure the table input job (requires mGatherer, mMapper and mReducer to be set):
    super.configureJob(job);

    // Some validation:
    if (getJobOutput() instanceof HFileMapReduceJobOutput) {
       if (mReducer instanceof IdentityReducer) {
         Preconditions.checkState(
             mGatherer.getOutputKeyClass() == HFileKeyValue.class,
             String.format(
                 "Gatherer '%s' writing HFiles must output HFileKeyValue keys, but got '%s'",
                 mGathererClass.getName(), mGatherer.getOutputKeyClass().getName()));
         Preconditions.checkState(
             mGatherer.getOutputValueClass() == NullWritable.class,
             String.format(
                 "Gatherer '%s' writing HFiles must output NullWritable values, but got '%s'",
                 mGathererClass.getName(), mGatherer.getOutputValueClass().getName()));
       }
       Preconditions.checkState(
           mReducer.getOutputKeyClass() == HFileKeyValue.class,
           String.format(
               "Reducer '%s' writing HFiles must output HFileKeyValue keys, but got '%s'",
               mReducerClass.getName(), mReducer.getOutputKeyClass().getName()));
       Preconditions.checkState(
           mReducer.getOutputValueClass() == NullWritable.class,
           String.format(
               "Reducer '%s' writing HFiles must output NullWritable values, but got '%s'",
               mReducerClass.getName(), mReducer.getOutputValueClass().getName()));
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    Map<String, KeyValueStore<?, ?>> requiredStores = new HashMap<String, KeyValueStore<?, ?>>();

    Map<String, KeyValueStore<?, ?>> gathererStores = mGatherer.getRequiredStores();
    if (null != gathererStores) {
      mergeStores(requiredStores, gathererStores);
    }

    if (null != mCombiner && mCombiner instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> combinerStores =
          ((KeyValueStoreClient) mCombiner).getRequiredStores();
      if (null != combinerStores) {
        mergeStores(requiredStores, combinerStores);
      }
    }

    if (null != mReducer && mReducer instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> reducerStores =
          ((KeyValueStoreClient) mReducer).getRequiredStores();
      if (null != reducerStores) {
        mergeStores(requiredStores, reducerStores);
      }
    }

    return requiredStores;
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
    return mCombiner;
  }

  /** {@inheritDoc} */
  @Override
  protected FijiReducer<?, ?, ?, ?> getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mGathererClass;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureOutput(Job job) throws IOException {
    final MapReduceJobOutput output = getJobOutput();
    if (null == output) {
      throw new JobConfigurationException("Must specify job output.");
    }
    final FijiReducer reducer = getReducer();

    if (output instanceof HFileMapReduceJobOutput) {
      if (reducer instanceof IdentityReducer) {
        output.configure(job);
      } else {
        // Cannot use the HFile output format if the reducer is not IdentityReducer:
        // Writing HFile from a Fiji reducer requires an extra map/reduce to sort the HFile keys.
        // This forces the output format of this MapReduce job to be SequenceFile.
        final HFileMapReduceJobOutput hfileOutput = (HFileMapReduceJobOutput) output;
        LOG.warn("Reducing to HFiles will require an extra MapReduce job.");
        new HFileReducerMapReduceJobOutput(hfileOutput).configure(job);
      }
    } else {
      output.configure(job);
    }
  }
}
