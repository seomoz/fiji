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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.impl.DirectFijiTableWriterContext;
import com.moz.fiji.schema.FijiURI;

/**
 * The class DirectFijiTableMapReduceJobOutput is used to indicate the usage of a Fiji table
 * as an output for a MapReduce job.
 *
 * <p>
 *   Use of this job output configuration is discouraged for many reasons:
 *   <ul>
 *     <li> It may induce a very high load on the target HBase cluster.
 *     <li> It may result in partial writes (eg. if the job fails half through).
 *   </ul>
 *   The recommended way to write to HBase tables is through the {@link HFileMapReduceJobOutput}.
 * </p>
 *
 * <h2>Configuring an output:</h2>
 * <p>
 *   DirectFijiTableMapReduceJobOutput must be configured with the address of the Fiji table to
 *   write to and optionally the number of reduce tasks to use for the job:
 * </p>
 * <pre>
 *   <code>
 *     final MapReduceJobOutput fijiTableOutput =
 *         MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(myURI);
 *   </code>
 * </pre>
 * @see HFileMapReduceJobOutput
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public class DirectFijiTableMapReduceJobOutput extends FijiTableMapReduceJobOutput {
  /** Default constructor. Accessible via {@link MapReduceJobOutputs}. */
  DirectFijiTableMapReduceJobOutput() {
  }

  /**
   * Creates a new <code>FijiTableMapReduceJobOutput</code> instance.
   *
   * @param tableURI The Fiji table to write to.
   */
  DirectFijiTableMapReduceJobOutput(FijiURI tableURI) {
    this(tableURI, 0);
  }

  /**
   * Creates a new <code>FijiTableMapReduceJobOutput</code> instance.
   *
   * @param tableURI The Fiji table to write to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   */
  DirectFijiTableMapReduceJobOutput(FijiURI tableURI, int numReduceTasks) {
    super(tableURI, numReduceTasks);
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // sets Hadoop output format, Fiji output table and # of reducers:
    super.configure(job);

    final Configuration conf = job.getConfiguration();

    // Fiji table context:
    conf.setClass(
        FijiConfKeys.KIJI_TABLE_CONTEXT_CLASS,
        DirectFijiTableWriterContext.class,
        FijiTableContext.class);

    // Since there's no "commit" operation for an entire map task writing to a
    // Fiji table, do not use speculative execution when writing directly to a Fiji table.
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    // No hadoop output:
    return NullOutputFormat.class;
  }
}
