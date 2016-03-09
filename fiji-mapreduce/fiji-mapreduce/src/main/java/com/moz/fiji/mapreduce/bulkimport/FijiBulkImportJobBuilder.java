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

package com.moz.fiji.mapreduce.bulkimport;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.JobConfigurationException;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.FijiMapper;
import com.moz.fiji.mapreduce.FijiReducer;
import com.moz.fiji.mapreduce.MapReduceJobInput;
import com.moz.fiji.mapreduce.MapReduceJobOutput;
import com.moz.fiji.mapreduce.bulkimport.impl.BulkImportMapper;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.framework.MapReduceJobBuilder;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.output.DirectFijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.HFileMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.FijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;

/** Builds a job that runs a FijiBulkImporter to import data into a Fiji table. */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiBulkImportJobBuilder
    extends MapReduceJobBuilder<FijiBulkImportJobBuilder> {

  /** The class of the bulk importer to run. */
  @SuppressWarnings("rawtypes")
  private Class<? extends FijiBulkImporter> mBulkImporterClass;

  /** The bulk importer instance. */
  private FijiBulkImporter<?, ?> mBulkImporter;
  /** The mapper instance to run (which runs the bulk importer inside it). */
  private FijiMapper<?, ?, ?, ?> mMapper;
  /** The reducer instance to run (may be null). */
  private FijiReducer<?, ?, ?, ?> mReducer;

  /** The job input. */
  private MapReduceJobInput mJobInput;

  /** Job output must be a Fiji table. */
  private FijiTableMapReduceJobOutput mJobOutput;

  /** Constructs a builder for jobs that run a FijiBulkImporter. */
  private FijiBulkImportJobBuilder() {
    mBulkImporterClass = null;

    mBulkImporter = null;
    mMapper = null;
    mReducer = null;

    mJobInput = null;
    mJobOutput = null;
  }

  /**
   * Creates a new builder for Fiji bulk import jobs.
   *
   * @return a new Fiji bulk import job builder.
   */
  public static FijiBulkImportJobBuilder create() {
    return new FijiBulkImportJobBuilder();
  }

  /**
   * Configures the job with input.
   *
   * @param jobInput The input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  public FijiBulkImportJobBuilder withInput(MapReduceJobInput jobInput) {
    mJobInput = jobInput;
    return this;
  }

  /**
   * Configures the bulk-importer output Fiji table.
   *
   * @param jobOutput Bulk importer must output to a Fiji table.
   * @return this builder.
   */
  public FijiBulkImportJobBuilder withOutput(FijiTableMapReduceJobOutput jobOutput) {
    mJobOutput = jobOutput;
    super.withOutput(jobOutput);
    return this;
  }

  /**
   * Configures the job output.
   *
   * @param jobOutput The output for the job.
   *     Bulk importer must output to a Fiji table.
   * @return This builder instance so you may chain configuration method calls.
   *
   * {@inheritDoc}
   */
  @Override
  public FijiBulkImportJobBuilder withOutput(MapReduceJobOutput jobOutput) {
    if (!(jobOutput instanceof FijiTableMapReduceJobOutput)) {
      throw new JobConfigurationException(String.format(
          "Invalid job output %s: expecting %s or %s",
          jobOutput.getClass().getName(),
          DirectFijiTableMapReduceJobOutput.class.getName(),
          HFileMapReduceJobOutput.class.getName()));
    }
    return withOutput((FijiTableMapReduceJobOutput) jobOutput);
  }

  /**
   * Configures the job with a bulk importer to run in the map phase.
   *
   * @param bulkImporterClass The bulk importer class to use in the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("rawtypes")
  public FijiBulkImportJobBuilder withBulkImporter(
      Class<? extends FijiBulkImporter> bulkImporterClass) {
    mBulkImporterClass = bulkImporterClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    final Configuration conf = job.getConfiguration();

    // Store the name of the the importer to use in the job configuration so the mapper can
    // create instances of it.
    // Construct the bulk importer instance.
    if (null == mBulkImporterClass) {
      throw new JobConfigurationException("Must specify a bulk importer.");
    }
    conf.setClass(
        FijiConfKeys.FIJI_BULK_IMPORTER_CLASS, mBulkImporterClass, FijiBulkImporter.class);

    mJobOutput.configure(job);

    // Configure the mapper and reducer. This part depends on whether we're going to write
    // to HFiles or directly to the table.
    configureJobForHFileOutput(job);

    job.setJobName("Fiji bulk import: " + mBulkImporterClass.getSimpleName());

    mBulkImporter = ReflectionUtils.newInstance(mBulkImporterClass, conf);

    // Configure the MapReduce job (requires mBulkImporter to be set properly):
    super.configureJob(job);
  }

  /**
   * Configures the job settings specific to writing HFiles.
   *
   * @param job The job to configure.
   */
  protected void configureJobForHFileOutput(Job job) {
    // Construct the mapper instance that runs the importer.
    mMapper = new BulkImportMapper<Object, Object>();

    // Don't need to do anything during the Reducer, but we need to run the reduce phase
    // so the KeyValue records output from the map phase get sorted.
    mReducer = new IdentityReducer<Object, Object>();
  }

  /** {@inheritDoc} */
  @Override
  protected FijiMapReduceJob build(Job job) {
    return FijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mBulkImporter.getRequiredStores();
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
    // Use no combiner.
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
    return mBulkImporterClass;
  }
}
