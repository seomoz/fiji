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

package com.moz.fiji.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.framework.MapReduceJobBuilder;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreClient;

/**
 * Builds general MapReduce jobs around Fiji mappers and reducers. For specialized tasks see
 * {@link com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder},
 * {@link com.moz.fiji.mapreduce.gather.FijiGatherJobBuilder},
 * {@link com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder}.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiMapReduceJobBuilder extends MapReduceJobBuilder<FijiMapReduceJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiMapReduceJobBuilder.class);

  /** The class of the mapper to run. */
  @SuppressWarnings("rawtypes")
  private Class<? extends FijiMapper> mMapperClass;
  /** The class of the combiner to run. */
  @SuppressWarnings("rawtypes")
  private Class<? extends FijiReducer> mCombinerClass;
  /** The class of the reducer to run. */
  @SuppressWarnings("rawtypes")
  private Class<? extends FijiReducer> mReducerClass;

  /** The mapper instance. */
  private FijiMapper<?, ?, ?, ?> mMapper;
  /** The combiner instance (may be null if no combiner is specified). */
  private FijiReducer<?, ?, ?, ?> mCombiner;
  /** The reducer instance (may be null if no reducer is specified). */
  private FijiReducer<?, ?, ?, ?> mReducer;

  /** The job input. */
  private MapReduceJobInput mJobInput;

  /** Constructs a builder for jobs that run a MapReduce job to transform data. */
  private FijiMapReduceJobBuilder() {
    mMapperClass = null;
    mCombinerClass = null;
    mReducerClass = null;

    mMapper = null;
    mCombiner = null;
    mReducer = null;

    mJobInput = null;
  }

  /**
   * Creates a new builder for Fiji transform jobs.
   *
   * @return a new Fiji transform job builder.
   */
  public static FijiMapReduceJobBuilder create() {
    return new FijiMapReduceJobBuilder();
  }

  /**
   * Configures the job with input.
   *
   * @param jobInput The input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  public FijiMapReduceJobBuilder withInput(MapReduceJobInput jobInput) {
    mJobInput = jobInput;
    return this;
  }

  /**
   * Configures the job with a mapper to run in the map phase.
   *
   * @param mapperClass The mapper class.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("rawtypes")
  public FijiMapReduceJobBuilder withMapper(Class<? extends FijiMapper> mapperClass) {
    mMapperClass = mapperClass;
    return this;
  }

  /**
   * Configures the job with a combiner to run (optional).
   *
   * @param combinerClass The combiner class.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("rawtypes")
  public FijiMapReduceJobBuilder withCombiner(Class<? extends FijiReducer> combinerClass) {
    mCombinerClass = combinerClass;
    return this;
  }

  /**
   * Configures the job with a reducer to run (optional).
   *
   * @param reducerClass The reducer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("rawtypes")
  public FijiMapReduceJobBuilder withReducer(Class<? extends FijiReducer> reducerClass) {
    mReducerClass = reducerClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Check that job input was configured.
    if (null == mJobInput) {
      throw new JobConfigurationException("Must specify job input.");
    }

    // Construct the mapper instance.
    if (null == mMapperClass) {
      throw new JobConfigurationException("Must specify a mapper.");
    }
    mMapper = ReflectionUtils.newInstance(mMapperClass, job.getConfiguration());

    // Construct the combiner instance (if specified).
    if (null != mCombinerClass) {
      mCombiner = ReflectionUtils.newInstance(mCombinerClass, job.getConfiguration());
    }

    // Construct the reducer instance (if specified).
    if (null != mReducerClass) {
      mReducer = ReflectionUtils.newInstance(mReducerClass, job.getConfiguration());
    }

    StringBuilder name = new StringBuilder();
    name.append("Fiji transform: ");
    name.append(mMapperClass.getSimpleName());
    if (null != mReducerClass) {
      name.append(" / ");
      name.append(mReducerClass.getSimpleName());
    }
    job.setJobName(name.toString());

    // Configure the MapReduce job.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    Map<String, KeyValueStore<?, ?>> requiredStores = new HashMap<String, KeyValueStore<?, ?>>();
    if (mMapper instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> mapperStores =
          ((KeyValueStoreClient) mMapper).getRequiredStores();
      if (null != mapperStores) {
        mergeStores(requiredStores, mapperStores);
      }
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
  protected MapReduceJobInput getJobInput() {
    return mJobInput;
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
    return mMapperClass;
  }
}
