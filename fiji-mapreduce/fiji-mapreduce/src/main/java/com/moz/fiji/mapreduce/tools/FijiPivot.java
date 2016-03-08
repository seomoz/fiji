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
import com.moz.fiji.mapreduce.MapReduceJobInput;
import com.moz.fiji.mapreduce.MapReduceJobOutput;
import com.moz.fiji.mapreduce.input.FijiTableMapReduceJobInput;
import com.moz.fiji.mapreduce.output.DirectFijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.HFileMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.FijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.pivot.FijiPivotJobBuilder;
import com.moz.fiji.mapreduce.pivot.impl.FijiPivoters;
import com.moz.fiji.mapreduce.tools.framework.FijiJobTool;
import com.moz.fiji.mapreduce.tools.framework.MapReduceJobInputFactory;
import com.moz.fiji.mapreduce.tools.framework.MapReduceJobOutputFactory;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.tools.FijiToolLauncher;
import com.moz.fiji.schema.tools.RequiredFlagException;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Runs a FijiPivoter a Fiji table.
 *
 * <p>
 * Here is an example of command-line to launch a FijiPivoter named {@code package.SomePivoter}
 * on the rows from an input table {@code fiji://.env/input_instance/input_table}
 * while writing cells to another output table {@code fiji://.env/output_instance/output_table}:
 * <pre><blockquote>
 *   $ fiji pivot \
 *       --pivoter='package.SomePivoter' \
 *       --input="format=fiji table=fiji://.env/default/input_table" \
 *       --output="format=fiji table=fiji://.env/default/output_table nsplits=5" \
 *       --lib=/path/to/libdir/
 * </blockquote></pre>
 * </p>
 */
@ApiAudience.Private
public final class FijiPivot extends FijiJobTool<FijiPivotJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiPivot.class);

  @Flag(name="pivoter",
      usage="FijiPivoter class to run over the table.")
  private String mPivoter= "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "pivot";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a pivoter over a Fiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  /** Fiji instance where the output table lives. */
  private Fiji mFiji;

  /** FijiTable to import data into. */
  private FijiTable mTable;

  /** Job output. */
  private FijiTableMapReduceJobOutput mOutput;

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    // Parse --input and --output flags:
    //   --input is guaranteed to be a Fiji table, --output is not.
    super.validateFlags();

    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }
    if (mPivoter.isEmpty()) {
      throw new RequiredFlagException("pivoter");
    }
    if (mOutputFlag.isEmpty()) {
      throw new RequiredFlagException("output");
    }

    final MapReduceJobOutput mrJobOutput =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
    Preconditions.checkArgument(mrJobOutput instanceof FijiTableMapReduceJobOutput,
        "Pivot jobs output format must be 'hfile' or 'fiji', but got output spec '%s'.",
        mOutputFlag);
    mOutput = (FijiTableMapReduceJobOutput) mrJobOutput;

    Preconditions.checkArgument(mOutput.getOutputTableURI().getTable() != null,
        "Specify the table to import data into with --output=...");
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mFiji = Fiji.Factory.open(mOutput.getOutputTableURI(), getConf());
    mTable = mFiji.openTable(mOutput.getOutputTableURI().getTable());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mFiji);
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected FijiPivotJobBuilder createJobBuilder() {
    return FijiPivotJobBuilder.create();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(FijiPivotJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException {

    // Resolve job input:
    final MapReduceJobInput input =
        MapReduceJobInputFactory.create().fromSpaceSeparatedMap(mInputFlag);
    if (!(input instanceof FijiTableMapReduceJobInput)) {
      throw new RuntimeException(String.format(
          "Invalid pivot job input: '%s'; must input from a Fiji table.", mInputFlag));
    }
    final FijiTableMapReduceJobInput tableInput = (FijiTableMapReduceJobInput) input;

    // Resolve job output:
    final MapReduceJobOutput output =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
    if (!(output instanceof FijiTableMapReduceJobOutput)) {
      throw new RuntimeException(String.format(
          "Invalid pivot job output: '%s'; must output to a Fiji table.", mOutputFlag));
    }
    mOutput = (FijiTableMapReduceJobOutput) output;

    // Configure job:
    super.configure(jobBuilder);
    jobBuilder
        .withPivoter(FijiPivoters.forName(mPivoter))
        .withInputTable(tableInput.getInputTableURI())
        .withOutput(mOutput);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final int jobStatus = super.run(nonFlagArgs);

    // TODO: Make this a method of job outputs?
    if (mOutput instanceof DirectFijiTableMapReduceJobOutput) {
      if (jobStatus == 0) {
        LOG.info("Pivot job for table {} completed successfully.",
            mOutput.getOutputTableURI());
      } else {
        LOG.error("Pivot job failed, output table {} may have partial writes.",
            mOutput.getOutputTableURI());
      }
    } else if (mOutput instanceof HFileMapReduceJobOutput) {
      if (jobStatus == 0) {
        // Provide instructions for completing the bulk import.
        LOG.info("Pivot job completed successfully. "
            + "HFiles may now be bulk-loaded into table {} with: {}",
            mOutput.getOutputTableURI(),
            String.format("fiji bulk-load --table=%s", mOutput.getOutputTableURI()));
      } else {
        LOG.error(
            "Pivot job failed: HFiles for table {} were not generated successfully.",
            mOutput.getOutputTableURI());
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
    System.exit(new FijiToolLauncher().run(new FijiPivot(), args));
  }
}
