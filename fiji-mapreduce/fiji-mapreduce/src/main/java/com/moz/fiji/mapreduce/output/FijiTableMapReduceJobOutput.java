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

package com.moz.fiji.mapreduce.output;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.MapReduceJobOutput;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.tools.framework.JobIOConfKeys;
import com.moz.fiji.schema.FijiURI;

/** MapReduce job output configuration that outputs to a Fiji table. */
@ApiAudience.Public
@ApiStability.Evolving
public abstract class FijiTableMapReduceJobOutput extends MapReduceJobOutput {

  /** URI of the output table. */
  private FijiURI mTableURI;

  /** Number of reduce tasks to use. */
  private int mNumReduceTasks;

  /** Default constructor. Do not use directly. */
  protected FijiTableMapReduceJobOutput() {
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    mTableURI = FijiURI.newBuilder(params.get(JobIOConfKeys.TABLE_KEY)).build();
    mNumReduceTasks = Integer.parseInt(params.get(JobIOConfKeys.NSPLITS_KEY));
  }

  /**
   * Creates a new <code>FijiTableMapReduceJobOutput</code> instance.
   *
   * @param tableURI The fiji table to write output to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   */
  public FijiTableMapReduceJobOutput(FijiURI tableURI, int numReduceTasks) {
    mTableURI = tableURI;
    mNumReduceTasks = numReduceTasks;
  }

  /** @return the number of reducers. */
  public int getNumReduceTasks() {
    return mNumReduceTasks;
  }

  /** @return the URI of the configured output table. */
  public FijiURI getOutputTableURI() {
    return mTableURI;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // sets Hadoop output format according to getOutputFormatClass()
    super.configure(job);

    final Configuration conf = job.getConfiguration();
    conf.set(FijiConfKeys.FIJI_OUTPUT_TABLE_URI, mTableURI.toString());

    job.setNumReduceTasks(getNumReduceTasks());

    // Adds HBase dependency jars to the distributed cache so they appear on the task classpath:
    GenericTableMapReduceUtil.addAllDependencyJars(job);
  }

}
