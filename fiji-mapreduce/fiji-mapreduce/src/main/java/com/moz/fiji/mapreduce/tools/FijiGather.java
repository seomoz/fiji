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

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;

import com.moz.fiji.mapreduce.gather.FijiGatherJobBuilder;
import com.moz.fiji.mapreduce.gather.impl.FijiGatherers;
import com.moz.fiji.mapreduce.impl.FijiReducers;
import com.moz.fiji.mapreduce.tools.framework.FijiJobTool;
import com.moz.fiji.mapreduce.tools.framework.MapReduceJobOutputFactory;
import com.moz.fiji.schema.tools.FijiToolLauncher;
import com.moz.fiji.schema.tools.RequiredFlagException;

/** Command-line tool for running a FijiGatherer. */
@ApiAudience.Private
public final class FijiGather extends FijiJobTool<FijiGatherJobBuilder> {
  @Flag(name="gatherer", usage="Fully-qualified class name of the gatherer to run")
  private String mGathererName = "";

  @Flag(name="combiner", usage="Fully-qualifier class name of the combiner to use (optional)")
  private String mCombinerName = "";

  @Flag(name="reducer", usage="Fully-qualified class name of the reducer to run")
  private String mReducerName = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "gather";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a FijiGatherer over a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mGathererName.isEmpty()) {
      throw new RequiredFlagException("gatherer");
    }
  }

  @Override
  protected FijiGatherJobBuilder createJobBuilder() {
    return FijiGatherJobBuilder.create();
  }

  @Override
  protected void configure(FijiGatherJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException {
    super.configure(jobBuilder);
    jobBuilder.withGatherer(FijiGatherers.forName(mGathererName));

    if (!mCombinerName.isEmpty()) {
      jobBuilder.withCombiner(FijiReducers.forName(mCombinerName));
    }
    if (!mReducerName.isEmpty()) {
      jobBuilder.withReducer(FijiReducers.forName(mReducerName));
    }
    MapReduceJobOutputFactory outputFactory = MapReduceJobOutputFactory.create();
    jobBuilder.withOutput(outputFactory.fromSpaceSeparatedMap(mOutputFlag));
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new FijiGather(), args));
  }
}
