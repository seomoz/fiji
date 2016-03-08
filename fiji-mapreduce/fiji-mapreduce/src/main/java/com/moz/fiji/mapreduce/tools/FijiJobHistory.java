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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.mapreduce.avro.generated.JobHistoryEntry;
import com.moz.fiji.mapreduce.framework.JobHistoryFijiTable;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.tools.BaseTool;
import com.moz.fiji.schema.tools.FijiToolLauncher;

/** A tool that installs a job history table and lets you query individual jobs from it. */
@ApiAudience.Private
public final class FijiJobHistory extends BaseTool {
  @Flag(name="fiji", usage="URI of the Fiji instance to query.")
  private String mFijiURIFlag = KConstants.DEFAULT_URI;

  @Flag(name="job-id", usage="ID of the job to query.")
  private String mJobId = "";

  @Flag(name="verbose", usage="Include counters and configuration for a given job-id.")
  private boolean mVerbose = false;

  @Flag(name="get-counter", usage="Get the named counter for this job-id. Name must"
      + "be of the form group:counter-name.")
  private String mCounterName = "";

  @Flag(name="counter-names", usage="Get the names of existing counters for the job-id.")
  private boolean mGetCounterNames = false;

  /** URI of the Fiji instance to query. */
  private FijiURI mFijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "job-history";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Inspect or manipulate the MapReduce job history table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Admin";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument((mFijiURIFlag != null) && !mFijiURIFlag.isEmpty(),
        "Specify a Fiji instance with --fiji=fiji://hbase-address/fiji-instance");
    mFijiURI = FijiURI.newBuilder(mFijiURIFlag).build();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Fiji fiji = Fiji.Factory.open(mFijiURI, getConf());
    try {
      JobHistoryFijiTable jobHistoryTable = JobHistoryFijiTable.open(fiji);
      try {
        if (!mJobId.isEmpty()) {
          JobHistoryEntry data = jobHistoryTable.getJobDetails(mJobId);
          printEntry(data);
        } else {
          FijiRowScanner jobScanner = jobHistoryTable.getJobScanner();
          for (FijiRowData data : jobScanner) {
            String jobid = data.getMostRecentValue("info", "jobId").toString();
            printEntry(jobHistoryTable.getJobDetails(jobid));
            getPrintStream().printf("%n");
          }
          jobScanner.close();
        }
      } finally {
        jobHistoryTable.close();
      }
    } finally {
      fiji.release();
    }

    return SUCCESS;
  }

  /**
   * Prints a job details.
   *
   * @param entry a JobHistoryEntry retrieved from the JobHistoryTable.
   * @throws IOException If there is an error retrieving the counters.
   */
  private void printEntry(JobHistoryEntry entry) throws IOException {
    final PrintStream ps = getPrintStream();
    ps.printf("Job:\t\t%s%n", entry.getJobId());
    ps.printf("Name:\t\t%s%n", entry.getJobName());
    ps.printf("Started:\t%s%n", new Date(entry.getJobStartTime()));
    ps.printf("Ended:\t\t%s%n", new Date(entry.getJobEndTime()));
    ps.printf("End Status:\t\t%s%n", entry.getJobEndStatus());
    if (mVerbose) {
      // we don't print individual counters, instead pretty print the counters string we stored.
      printCounters(entry);
      printConfiguration(entry);
    }
    if (mGetCounterNames) {
      ps.println("Counters for this job:");
      ps.println(Arrays.toString(entry.getCountersFamily().keySet().toArray()));
    }
    if (!mCounterName.isEmpty()) {
      if (entry.getCountersFamily().containsKey(mCounterName)) {
        ps.println("Value for counter " + mCounterName + ":");
        ps.println(entry.getCountersFamily().get(mCounterName));
      } else {
        ps.println("Counter not found: " + mCounterName);
      }
    }
  }

  /**
   * Prints a representation of the Counters for a Job.
   *
   * @param entry a JobHistoryEntry retrieved from the JobHistoryTable.
   */
  private void printCounters(JobHistoryEntry entry) {
    PrintStream ps = getPrintStream();
    ps.println("Counters:");
    ps.println(entry.getJobCounters());
  }

  /**
   * Prints a representation of the Configuration for the Job.
   * @param entry a JobHistoryEntry retrieved from the JobHistoryTable.
   */
  private void printConfiguration(JobHistoryEntry entry) {
    PrintStream ps = getPrintStream();
    ps.println("Configuration:");
    ps.println(entry.getJobConfiguration());
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new FijiJobHistory(), args));
  }
}
