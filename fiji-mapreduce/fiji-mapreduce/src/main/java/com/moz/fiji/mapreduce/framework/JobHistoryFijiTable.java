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

package com.moz.fiji.mapreduce.framework;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.Maps;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.avro.generated.JobHistoryEntry;
import com.moz.fiji.schema.AtomicFijiPutter;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * A class providing an API to install and access the job history fiji table.
 *
 * Used in places like FijiMapReduceJob to record information about jobs run through Fiji.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class JobHistoryFijiTable implements Closeable {
  /** Every existing job history table has at least this version. */
  private static final String PREV_TABLE_LAYOUT_VERSION = "1";
  /** The name of the table storing a history of completed jobs. */
  private static final String TABLE_NAME = "job_history";
  /** The path to the layout for the table in our resources. */
  private static final String TABLE_LAYOUT_RESOURCE = "/com.moz.fiji/mapreduce/job-history-layout.json";
  /** JSON file for job history table that adds job counters family. */
  private static final String TABLE_LAYOUT_V2 =
      "/com.moz.fiji/mapreduce/job-history-layout-v2-counterfamily.json";

  /** Column family where job history information is stored. */
  public static final String JOB_HISTORY_FAMILY = "info";
  /** Column family for job counters. */
  public static final String JOB_HISTORY_COUNTERS_FAMILY = "counters";
  /** Column family where extended information is stored. */
  public static final String JOB_HISTORY_EXTENDED_INFO_FAMILY = "extendedInfo";
  /** Qualifier where job IDs are stored. */
  public static final String JOB_HISTORY_ID_QUALIFIER = "jobId";
  /** Qualifier where job names are stored. */
  public static final String JOB_HISTORY_NAME_QUALIFIER = "jobName";
  /** Qualifier where job start times are stored. */
  public static final String JOB_HISTORY_START_TIME_QUALIFIER = "startTime";
  /** Qualifier where job end times are stored. */
  public static final String JOB_HISTORY_END_TIME_QUALIFIER = "endTime";
  /** Qualifier where job end statuses are stored. */
  public static final String JOB_HISTORY_END_STATUS_QUALIFIER = "jobEndStatus";
  /** Qualifier where job counters are stored. */
  public static final String JOB_HISTORY_COUNTERS_QUALIFIER = "counters";
  /** Qualifier where job configurations are stored. */
  public static final String JOB_HISTORY_CONFIGURATION_QUALIFIER = "configuration";
  /** Value stored to configuration qualifier if the job did not have a configuration. */
  public static final String JOB_HISTORY_NO_CONFIGURATION_VALUE = "No configuration for job.";

  /** Message to record into the {@value #JOB_HISTORY_END_STATUS_QUALIFIER} on job success. */
  public static final String SUCCEEDED = "SUCCEEDED";
  /** Message to record into the {@value #JOB_HISTORY_END_STATUS_QUALIFIER} on job failure. */
  public static final String FAILED = "FAILED";

  /** The HBaseFijiTable managed by the JobHistoryFijiTable. */
  private final FijiTable mFijiTable;

  /**
   * Opens a JobHistoryFijiTable for a given fiji, installing it if necessary. This method should
   * be matched with a call to {@link #close}.
   *
   * @param fiji The fiji instance to use.
   * @return An opened JobHistoryFijiTable.
   * @throws IOException If there is an error opening the table.
   */
  public static JobHistoryFijiTable open(Fiji fiji) throws IOException {
    return new JobHistoryFijiTable(fiji);
  }

  /**
   * Returns the default name of the job history table.
   *
   * @return The name of the job history table as used by the installer.
   */
  public static String getInstallName() {
    return TABLE_NAME;
  }

  /**
   * Extract the counters from a Job.
   *
   * @param job Job from which to get counters.
   * @return a map from counters to their counts. Keys are group:name.
   * @throws IOException in case of an error getting the counters.
   */
  private static Map<String, Long> getCounters(
      final Job job
  ) throws IOException {
    final Counters counters = job.getCounters();
    final Map<String, Long> countersMap = Maps.newHashMap();
    for (String group : counters.getGroupNames()) {
      for (Counter counter : counters.getGroup(group)) {
        countersMap.put(String.format("%s:%s", group, counter.getName()), counter.getValue());
      }
    }
    return countersMap;
  }

  /**
   * Add counters to an outstanding atomic transaction on the given atomic putter.
   *
   * @param putter atomic putter with an open transaction.
   * @param startTime time in milliseconds since the epoch at which the job started.
   * @param counters map of counters from the job. Keys should be of the form 'group:name'.
   * @throws IOException in case of an error adding the counters to the transaction.
   */
  private static void writeCounters(
      final AtomicFijiPutter putter,
      final long startTime,
      final Map<String, Long> counters
  ) throws IOException {
    for (Map.Entry<String, Long> counterEntry : counters.entrySet()) {
      putter.put(JOB_HISTORY_COUNTERS_FAMILY, counterEntry.getKey(), startTime,
          counterEntry.getValue());
    }
  }

  /**
   * Add extended information to an outstanding atomic transaction on the given atomic putter.
   *
   * @param putter atomic putter with an open transaction.
   * @param startTime time in milliseconds since the epoch at which the job started.
   * @param extendedInfo map of additional information about the job.
   * @throws IOException in case of an error adding the extended info to the transaction.
   */
  private static void writeExtendedInfo(
      final AtomicFijiPutter putter,
      final long startTime,
      final Map<String, String> extendedInfo
  ) throws IOException {
    for (Map.Entry<String, String> infoEntry : extendedInfo.entrySet()) {
      putter.put(JOB_HISTORY_EXTENDED_INFO_FAMILY, infoEntry.getKey(), startTime,
          infoEntry.getValue());
    }
  }

  /**
   * Private constructor that opens a new JobHistoryFijiTable, creating it if necessary.
   * This method also updates an existing layout to the latest layout for the job
   * history table.
   *
   * @param fiji The fiji instance to retrieve the job history table from.
   * @throws IOException If there's an error opening the underlying HBaseFijiTable.
   */
  private JobHistoryFijiTable(Fiji fiji) throws IOException {
    install(fiji);
    mFijiTable = fiji.openTable(TABLE_NAME);
  }

  /**
   * Writes a job into the JobHistoryFijiTable.
   *
   * @param job The job to save.
   * @param startTime The time the job began, in milliseconds.
   * @param endTime The time the job ended, in milliseconds
   * @throws IOException If there is an error writing to the table.
   */
  public void recordJob(
      final Job job,
      final long startTime,
      final long endTime
  ) throws IOException {
    recordJob(
        job.getJobID().toString(),
        job.getJobName(),
        startTime,
        endTime,
        job.isSuccessful(),
        job.getConfiguration(),
        getCounters(job),
        Collections.<String, String>emptyMap());
  }

  /**
   * Writes details of a job into the JobHistoryFijiTable.
   *
   * @param jobId unique identifier for the job.
   * @param jobName name of the job.
   * @param startTime time in milliseconds since the epoch at which the job started.
   * @param endTime time in milliseconds since the epoch at which the job ended.
   * @param jobSuccess whether the job completed successfully.
   * @param counters map of counters from the job. Keys should be of the form 'group:name'.
   * @param conf Configuration of the job.
   * @param extendedInfo any additional information which should be stored about the job.
   * @throws IOException in case of an error writing to the table.
   */
  // CSOFF: ParameterNumberCheck
  public void recordJob(
      final String jobId,
      final String jobName,
      final long startTime,
      final long endTime,
      final boolean jobSuccess,
      final Configuration conf,
      final Map<String, Long> counters,
      final Map<String, String> extendedInfo
  ) throws IOException {
    // CSON: ParameterNumberCheck
    final EntityId eid = mFijiTable.getEntityId(jobId);
    final AtomicFijiPutter putter = mFijiTable.getWriterFactory().openAtomicPutter();
    try {
      putter.begin(eid);
      putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_ID_QUALIFIER, startTime, jobId);
      putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_NAME_QUALIFIER, startTime, jobName);
      putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_START_TIME_QUALIFIER, startTime, startTime);
      putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_END_TIME_QUALIFIER, startTime, endTime);
      putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_END_STATUS_QUALIFIER, startTime,
          (jobSuccess) ? SUCCEEDED : FAILED);
      putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_COUNTERS_QUALIFIER, startTime,
          counters.toString());
      if (null != conf) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        conf.writeXml(baos);
        putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_CONFIGURATION_QUALIFIER, startTime,
            baos.toString("UTF-8"));
      } else {
        putter.put(JOB_HISTORY_FAMILY, JOB_HISTORY_CONFIGURATION_QUALIFIER, startTime,
            JOB_HISTORY_NO_CONFIGURATION_VALUE);
      }
      writeCounters(putter, startTime, counters);
      writeExtendedInfo(putter, startTime, extendedInfo);
      putter.commit();
    } finally {
      putter.close();
    }
  }

  /**
   * Install the job history table into a Fiji instance. This should be called only
   * via open, because we might want to update the layout of the job history table.
   *
   * @param fiji The Fiji instance to install this table in.
   * @throws IOException If there is an error.
   */
  private static void install(Fiji fiji) throws IOException {
    if (!fiji.getTableNames().contains(TABLE_NAME)) {
      // Try to install the job history table if necessary.
      fiji.createTable(
          FijiTableLayout.createFromEffectiveJsonResource(TABLE_LAYOUT_RESOURCE).getDesc());
    }
    // At this point, we either have an existing table or we just installed a new
    // one. Check if the table is using the old layout, and update it if it is.
    if (fiji.getMetaTable().getTableLayout(TABLE_NAME).getDesc().getLayoutId()
        .equals(PREV_TABLE_LAYOUT_VERSION)) {
      FijiTableLayout ktl = FijiTableLayout
          .createFromEffectiveJsonResource(TABLE_LAYOUT_V2);
      fiji.modifyTableLayout(ktl.getDesc());
    }
    // If there are further updates to the job history layout, they should probably be added here.
  }

  /**
   * Get the saved information for a particular JobID.
   *
   * @param jobId The id of the job to retrieve.
   * @return A FijiRowData containing all the information for the requested Job.
   * @throws IOException If there is an IO error retrieving the data.
   */
  public JobHistoryEntry getJobDetails(String jobId) throws IOException {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().addFamily("info")
        .addFamily("counters")
        .addFamily("extendedInfo");
    final FijiDataRequest request = builder.build();
    final FijiRowData data;
    final FijiTableReader reader = mFijiTable.openTableReader();
    try {
      data = reader.get(mFijiTable.getEntityId(jobId), request);
    } finally {
      reader.close();
    }

    // We have to pull out the maps here to get around a pickiness for the Java compiler because
    // getMostRecentValues returns a generic type, which causes a compile error while passing to
    // setExtendedInfo below.
    NavigableMap<String, String> tempExtMap = data.getMostRecentValues("extendedInfo");
    NavigableMap<String, Long> tempCounterMap = data.getMostRecentValues("counters");

    return JobHistoryEntry.newBuilder()
        .setJobId(data.getMostRecentValue("info", "jobId").toString())
        .setJobName(data.getMostRecentValue("info", "jobName").toString())
        .setJobStartTime(data.<Long>getMostRecentValue("info", "startTime"))
        .setJobEndTime(data.<Long>getMostRecentValue("info", "endTime"))
        .setJobEndStatus(data.getMostRecentValue("info", "jobEndStatus").toString())
        .setJobCounters(data.getMostRecentValue("info", "counters").toString())
        .setJobConfiguration(data.getMostRecentValue("info", "configuration").toString())
        .setExtendedInfo(tempExtMap)
        .setCountersFamily(tempCounterMap)
        .build();
  }

  /**
   * Get the saved information for all JobIDs.
   *
   * @return A FijiRowScanner containing details for all the JobIDs.
   * @throws IOException If there is an IO error retrieving the data.
   */
  public FijiRowScanner getJobScanner() throws IOException {
    FijiDataRequest wdr = FijiDataRequest.create("info");

    FijiTableReader wtr = mFijiTable.openTableReader();
    try {
      return wtr.getScanner(wdr);
    } finally {
      wtr.close();
    }
  }

  @Override
  public void close() throws IOException {
    mFijiTable.release();
  }
}
