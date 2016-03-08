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

package com.moz.fiji.mapreduce.tools;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;

import com.moz.fiji.mapreduce.output.DirectFijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.HFileMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.FijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder;
import com.moz.fiji.mapreduce.produce.impl.FijiProducers;
import com.moz.fiji.mapreduce.tools.framework.FijiJobTool;
import com.moz.fiji.schema.tools.FijiToolLauncher;

/** Program for running a Fiji producer in a MapReduce job. */
@ApiAudience.Private
public final class FijiProduce extends FijiJobTool<FijiProduceJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiProduce.class);

  @Flag(name = "producer", usage = "Fully-qualified class name of the producer to run")
  private String mProducerName = "";

  @Flag(name = "num-threads", usage = "Positive integer number of threads to use")
  private int mNumThreadsPerMapper = 1;

  /** Producer must output to a Fiji table, and the output table must be the input table. */
  private FijiTableMapReduceJobOutput mOutput;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "produce";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a FijiProducer over a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  @Override
  protected void validateFlags() throws Exception {
    // Parse --input and --output flags, --input is guaranteed to be a Fiji table:
    super.validateFlags();

    Preconditions.checkArgument(!mProducerName.isEmpty(),
        "Specify a producer with --producer=...");
    Preconditions.checkArgument(mNumThreadsPerMapper >= 1,
        "Illegal number of threads per mapper: {}, must", mNumThreadsPerMapper);

    Preconditions.checkArgument(getJobOutput() instanceof FijiTableMapReduceJobOutput,
        "Producer must output to a Fiji table but got {}.", getJobOutput().getClass().getName());
    mOutput = (FijiTableMapReduceJobOutput) getJobOutput();
    Preconditions.checkArgument(
        mOutput.getOutputTableURI().equals(getJobInputTable().getInputTableURI()),
        "Producer job output table {} does not match input table {}",
        mOutput.getOutputTableURI(), getJobInputTable().getInputTableURI());
  }

  /** {@inheritDoc} */
  @Override
  protected FijiProduceJobBuilder createJobBuilder() {
    return FijiProduceJobBuilder.create();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(FijiProduceJobBuilder jobBuilder) throws ClassNotFoundException,
      IOException {
    // Configure job input:
    super.configure(jobBuilder);

    jobBuilder
        .withProducer(FijiProducers.forName(mProducerName))
        .withOutput(mOutput)
        .withNumThreads(mNumThreadsPerMapper);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final int jobStatus = super.run(nonFlagArgs);

    // TODO: Move this to job outputs?
    if (mOutput instanceof DirectFijiTableMapReduceJobOutput) {
      if (jobStatus == 0) {
        LOG.info("Producer {} for table {} completed successfully.",
            mProducerName, mOutput.getOutputTableURI());
      } else {
        LOG.error("Producer {} failed: table {} may have partial writes.",
            mProducerName, mOutput.getOutputTableURI());
      }
    } else if (mOutput instanceof HFileMapReduceJobOutput) {
      if (jobStatus == 0) {
        LOG.info("Producer {} for table {} completed successfully and wrote HFiles. "
            + "HFiles may now be loaded with: {}",
            mProducerName, mOutput.getOutputTableURI(),
            String.format("fiji bulk-load --table=%s", mOutput.getOutputTableURI()));
      } else {
        LOG.error("Producer {} for table {} failed: HFiles were not generated properly.",
            mProducerName, mOutput.getOutputTableURI());
      }
    } else {
      LOG.error("Unknown job output format: {}", mOutput.getClass().getName());
    }
    return jobStatus;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new FijiProduce(), args));
  }
}
