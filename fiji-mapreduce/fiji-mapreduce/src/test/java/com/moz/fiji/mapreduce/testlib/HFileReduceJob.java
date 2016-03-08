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

package com.moz.fiji.mapreduce.testlib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.common.flags.FlagParser;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.FijiMapReduceJobBuilder;
import com.moz.fiji.mapreduce.FijiMapper;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;

/** Processes the output of a FijiTableReducer that intends to generate HFiles. */
public final class HFileReduceJob {
  @Flag(
      name = "input-path",
      usage = "Path to the input SequenceFile<HFileKeyValue, NullWritable>.")
  private String mInputPath;

  @Flag(
      name = "output-path",
      usage = "Write the output HFiles under this path.")
  private String mOutputPath;

  @Flag(
      name = "output-table",
      usage = "FijiURI of the target table to write to.")
  private String mOutputTable;

  @Flag(
      name = "nsplits",
      usage = "Number of splits")
  private int mNumSplits = 0;

  /** An identity mapper. */
  private static class IdentityMapper
      extends FijiMapper<HFileKeyValue, NullWritable, HFileKeyValue, NullWritable> {

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
    protected void map(HFileKeyValue key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public void run(String[] args) throws Exception {
    FlagParser.init(this,  args);

    final Path inputPath = new Path(mInputPath);
    final Path outputPath = new Path(mOutputPath);
    final FijiURI tableURI = FijiURI.newBuilder(mOutputTable).build();
    final Fiji fiji = Fiji.Factory.open(tableURI);
    final FijiTable table = fiji.openTable(tableURI.getTable());

    final FijiMapReduceJob mrjob = FijiMapReduceJobBuilder.create()
        .withConf(new Configuration())  // use MapReduce cluster from local environment
        .withInput(MapReduceJobInputs.newSequenceFileMapReduceJobInput(inputPath))
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            table.getURI(), outputPath, mNumSplits))
        .withMapper(IdentityMapper.class)
        .withReducer(IdentityReducer.class)
        .build();

    if (!mrjob.run()) {
      System.err.println("Job failed.");
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    new HFileReduceJob().run(args);
  }
}
