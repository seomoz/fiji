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
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.bulkimport.impl.FijiBulkImporters;
import com.moz.fiji.mapreduce.output.DirectFijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.HFileMapReduceJobOutput;
import com.moz.fiji.mapreduce.output.FijiTableMapReduceJobOutput;
import com.moz.fiji.mapreduce.tools.framework.JobTool;
import com.moz.fiji.mapreduce.tools.framework.MapReduceJobInputFactory;
import com.moz.fiji.mapreduce.tools.framework.MapReduceJobOutputFactory;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.tools.FijiToolLauncher;
import com.moz.fiji.schema.tools.RequiredFlagException;
import com.moz.fiji.schema.util.ResourceUtils;

/** Bulk imports a file into a Fiji table. */
@ApiAudience.Private
public final class FijiBulkImport extends JobTool<FijiBulkImportJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiBulkImport.class);

  @Flag(name="importer", usage="FijiBulkImporter class to use")
  private String mImporter = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "bulk-import";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Bulk import data into a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Bulk";
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
    // Do NOT call super.validateFlags()

    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }
    if (mImporter.isEmpty()) {
      throw new RequiredFlagException("importer");
    }
    if (mOutputFlag.isEmpty()) {
      throw new RequiredFlagException("output");
    }

    // Make sure input flag has necessary components.
    Preconditions.checkArgument(mInputFlag.contains("format="),
        "Specify input format with format=... in the --input flag.");
    Preconditions.checkArgument(mInputFlag.contains("file="),
        "Specify input file to import data from with file=... in the --input flag.");

    // Make sure output flag has necessary components.
    Preconditions.checkArgument(mOutputFlag.contains("nsplits="),
        "Specify splits with nsplits=... in the --output flag.");
    Preconditions.checkArgument(mOutputFlag.contains("format="),
        "Specify destination format with format=... in the --output flag.");
    Preconditions.checkArgument(mOutputFlag.contains("format="),
        "Specify the table to import data into with table=... in the --output flag.");
    // Either format is fiji xor format is hfile with the file path specified.
    Preconditions.checkArgument(mOutputFlag.contains("format=fiji")
        ^ (mOutputFlag.contains("format=hfile") && mOutputFlag.contains("file=")),
        "For outputting to HFiles, specify path with file=... in the --output flag.");

    final MapReduceJobOutput mrJobOutput =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
    Preconditions.checkArgument(mrJobOutput instanceof FijiTableMapReduceJobOutput,
        "Bulk-import jobs output format must be 'hfile' or 'fiji', but got output spec '%s'.",
        mOutputFlag);
    mOutput = (FijiTableMapReduceJobOutput) mrJobOutput;

    Preconditions.checkArgument(mOutput.getOutputTableURI().getTable() != null,
        "Specify the table to import data into with table=... in --output flag.");
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
  protected FijiBulkImportJobBuilder createJobBuilder() {
    return FijiBulkImportJobBuilder.create();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(FijiBulkImportJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException {

    // Resolve job input:
    final MapReduceJobInput input =
        MapReduceJobInputFactory.create().fromSpaceSeparatedMap(mInputFlag);

    // Resolve job output:
    final MapReduceJobOutput output =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
    if (!(output instanceof FijiTableMapReduceJobOutput)) {
      throw new RuntimeException(String.format(
          "Invalid bulk-importer job output: '%s'; must output to a Fiji table.", mOutputFlag));
    }
    mOutput = (FijiTableMapReduceJobOutput) output;

    // Configure job:
    super.configure(jobBuilder);
    jobBuilder
        .withBulkImporter(FijiBulkImporters.forName(mImporter))
        .withInput(input)
        .withOutput(mOutput);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final int jobStatus = super.run(nonFlagArgs);

    // TODO: Make this a method of job outputs?
    if (mOutput instanceof DirectFijiTableMapReduceJobOutput) {
      if (jobStatus == 0) {
        LOG.info("Bulk-import job for table {} completed successfully.",
            mOutput.getOutputTableURI());
      } else {
        LOG.error("Bulk-import job failed, output table {} may have partial writes.",
            mOutput.getOutputTableURI());
      }
    } else if (mOutput instanceof HFileMapReduceJobOutput) {
      if (jobStatus == 0) {
        // Provide instructions for completing the bulk import.
        LOG.info("Bulk-import job completed successfully. "
            + "HFiles may now be bulk-loaded into table {} with: {}",
            mOutput.getOutputTableURI(),
            String.format("fiji bulk-load --table=%s", mOutput.getOutputTableURI()));
      } else {
        LOG.error(
            "Bulk-importer job failed: HFiles for table {} were not generated successfully.",
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
    System.exit(new FijiToolLauncher().run(new FijiBulkImport(), args));
  }
}
