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

package com.moz.fiji.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.framework.JobHistoryFijiTable;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiURI;

/** A runnable MapReduce job that interacts with Fiji tables. */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiMapReduceJob {
  private static final Logger LOG = LoggerFactory.getLogger(FijiMapReduceJob.class);

  /** The wrapped Hadoop Job. */
  private final Job mJob;
  // TODO(KIJIMR-92): Versions of Hadoop after 20.x add the ability to get start and
  // end times directly from a Job, making these superfluous.
  /** Used to track when the job's execution begins. */
  private long mJobStartTime;
  /** Used to track when the job's execution ends. */
  private long mJobEndTime;
  /** Completion polling thread. */
  private Thread mCompletionPollingThread;
  /** Whether the job is currently started. */
  private boolean mJobStarted = false;

  /**
   * Creates a new <code>FijiMapReduceJob</code> instance.
   *
   * @param job The Hadoop job to run.
   */
  private FijiMapReduceJob(Job job) {
    mJob = Preconditions.checkNotNull(job);
  }

  /**
   * Creates a new <code>FijiMapReduceJob</code>.
   *
   * @param job is a Hadoop {@link Job} that interacts with Fiji and will be wrapped by the new
   *     <code>FijiMapReduceJob</code>.
   * @return A new <code>FijiMapReduceJob</code> backed by a Hadoop {@link Job}.
   */
  public static FijiMapReduceJob create(Job job) {
    return new FijiMapReduceJob(job);
  }

  /**
   * Join the completion polling thread and block until it exits.
   *
   * @throws InterruptedException if the completion polling thread is interrupted.
   * @throws IOException in case of an IO error when querying job success.
   * @return Whether the job completed successfully
   */
  public boolean join() throws InterruptedException, IOException {
    Preconditions.checkState(mJobStarted,
        "Cannot join completion polling thread because the job is not running.");
    mCompletionPollingThread.join();
    return mJob.isSuccessful();
  }

  /**
   * The status of a job that was started asynchronously using {@link #submit()}.
   */
  public static final class Status {
    /** The Job whose status is being tracked. */
    private final Job mJob;

    /**
     * Constructs a <code>Status</code> around a Hadoop job.
     *
     * @param job The Hadoop job being run.
     */
    protected Status(Job job) {
      mJob = job;
    }

    /**
     * Determines whether the job has completed.
     *
     * @return Whether the job has completed.
     * @throws IOException If there is an error querying the job.
     */
    public boolean isComplete() throws IOException {
      return mJob.isComplete();
    }

    /**
     * Determines whether the job was successful.  The return value is undefined if the
     * job has not yet completed.
     *
     * @return Whether the job was successful.
     * @throws IOException If there is an error querying the job.
     */
    public boolean isSuccessful() throws IOException {
      return mJob.isSuccessful();
    }
  }

  /**
   * Records information about a completed job into the history table of a Fiji instance.
   *
   * If the attempt fails due to an IOException (a Fiji cannot be made, there is no job history
   * table, its content is corrupted, etc.), we catch the exception and warn the user.
   *
   * @param fiji Fiji instance to write the job record to.
   * @throws IOException on I/O error.
   */
  private void recordJobHistory(Fiji fiji) throws IOException {
    final Job job = getHadoopJob();
    JobHistoryFijiTable jobHistory = null;
    try {
      jobHistory = JobHistoryFijiTable.open(fiji);
      jobHistory.recordJob(job, mJobStartTime, mJobEndTime);
    } catch (IOException ioe) {
      // We swallow errors for recording jobs, because it's a non-fatal error for the task.
        LOG.warn(
            "Error recording job {} in history table of Fiji instance {}:\n"
            + "{}\n"
            + "This does not affect the success of job {}.\n",
            getHadoopJob().getJobID(), fiji.getURI(),
            StringUtils.stringifyException(ioe),
            getHadoopJob().getJobID());
    } finally {
      IOUtils.closeQuietly(jobHistory);
    }
  }

  /**
   * Records information about a completed job into all relevant Fiji instances.
   *
   * Underlying failures are logged but not fatal.
   */
  private void recordJobHistory() {
    final Configuration conf = getHadoopJob().getConfiguration();
    final Set<FijiURI> instanceURIs = Sets.newHashSet();
    if (conf.get(FijiConfKeys.KIJI_INPUT_TABLE_URI) != null) {
        instanceURIs.add(FijiURI.newBuilder(conf.get(FijiConfKeys.KIJI_INPUT_TABLE_URI))
            .withTableName(null)
            .withColumnNames(Collections.<String>emptyList())
            .build());
    }
    if (conf.get(FijiConfKeys.KIJI_OUTPUT_TABLE_URI) != null) {
        instanceURIs.add(FijiURI.newBuilder(conf.get(FijiConfKeys.KIJI_OUTPUT_TABLE_URI))
            .withTableName(null)
            .withColumnNames(Collections.<String>emptyList())
            .build());
    }

    for (FijiURI instanceURI : instanceURIs) {
      if (instanceURI != null) {
        try {
          final Fiji fiji = Fiji.Factory.open(instanceURI, conf);
          try {
            recordJobHistory(fiji);
          } finally {
            fiji.release();
          }
        } catch (IOException ioe) {
          LOG.warn(
              "Error recording job {} in history table of Fiji instance {}: {}\n"
              + "This does not affect the success of job {}.",
              getHadoopJob().getJobID(),
              instanceURI,
              ioe.getMessage(),
              getHadoopJob().getJobID());
        }
      }
    }
  }

  /**
   * Gives access the underlying Hadoop Job object.
   *
   * @return The Hadoop Job object.
   */
  public Job getHadoopJob() {
    return mJob;
  }

  // Unfortunately, our use of an anonymous inner class in this method confuses checkstyle.
  // We disable it temporarily.
  // CSOFF: VisibilityModifierCheck
  /**
   * Starts the job and return immediately.
   *
   * @return The job status. This can be queried for completion and success or failure.
   * @throws ClassNotFoundException If a required class cannot be found on the classpath.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the thread is interrupted.
   */
  public Status submit() throws ClassNotFoundException, IOException, InterruptedException {
    mJobStarted = true;
    mJobStartTime = System.currentTimeMillis();
    LOG.debug("Submitting job");
    mJob.submit();
    final Status jobStatus = new Status(mJob);

    // We use an inline defined thread here to poll jobStatus for completion to update
    // our job history table.
    mCompletionPollingThread = new Thread() {
      // Polling interval in milliseconds.
      private final int mPollInterval =
          mJob.getConfiguration().getInt(FijiConfKeys.KIJI_MAPREDUCE_POLL_INTERVAL, 1000);

      public void run() {
        try {
          while (!jobStatus.isComplete()) {
            Thread.sleep(mPollInterval);
          }
          mJobEndTime = System.currentTimeMillis();
          // IOException in recordJobHistory() are caught and logged.
          recordJobHistory();
        } catch (IOException e) {
          // If we catch an error while polling, bail out.
          LOG.debug("Error polling jobStatus.");
          return;
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while polling jobStatus.");
          return;
        }
      }
    };

    mCompletionPollingThread.setDaemon(true);
    mCompletionPollingThread.start();
    return jobStatus;
  }

  // CSON: VisibilityModifierCheck
  /**
   * Runs the job (blocks until it is complete).
   *
   * @return Whether the job was successful.
   * @throws ClassNotFoundException If a required class cannot be found on the classpath.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the thread is interrupted.
   */
  public boolean run() throws ClassNotFoundException, IOException, InterruptedException {
    mJobStartTime = System.currentTimeMillis();
    LOG.debug("Running job");
    boolean ret = mJob.waitForCompletion(true);
    mJobEndTime = System.currentTimeMillis();
    try {
      recordJobHistory();
    } catch (Throwable thr) {
      thr.printStackTrace();
    }
    return ret;
  }
}
