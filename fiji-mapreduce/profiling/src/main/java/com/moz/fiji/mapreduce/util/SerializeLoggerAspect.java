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

package com.moz.fiji.mapreduce.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.StringUtils;
import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.util.LogTimerAspect;
import com.moz.fiji.schema.util.LoggingInfo;

/**
 * This aspect is invoked after the cleanup function in a mapreduce job. It
 * accesses logging information gathered by the LogTimerAspect in fiji schema and
 * serializes it to a local file.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Aspect
public class SerializeLoggerAspect {
  private MRLogTimerAspect mMRLogTimerAspect;
  private LogTimerAspect mLogTimerAspect;
  private static final Logger LOG = LoggerFactory.getLogger(SerializeLoggerAspect.class);
  /**
   * Output directory (relative to the MapReduce job's working directory on HDFS, that is
   * context.getWorkingDirectory()) to store profiling results.
   */
  private static final String STATS_DIR = "fijistats";

  /**
   * Default constructor. Initializes the singleton LogTimerAspect for this JVM instance.
   */
  protected SerializeLoggerAspect() {
    if (Aspects.hasAspect(LogTimerAspect.class)) {
      mLogTimerAspect = Aspects.aspectOf(LogTimerAspect.class);
    } else {
      throw new RuntimeException("Log Timer aspect not found!");
    }

    if (Aspects.hasAspect(MRLogTimerAspect.class)) {
      mMRLogTimerAspect = Aspects.aspectOf(MRLogTimerAspect.class);
    } else {
      throw new RuntimeException("MR Log Timer aspect not found!");
    }
  }

  /**
   * Pointcut attached to cleanup code in MapReduce tasks. We can have a number of classes
   * that inherit from FijiMapper/Reducer. We would like to match these only once, so that
   * the code is not executed once for the parent and once for the child class. This along
   * with the advice below that uses cflowbelow was the cleanest way I could find to do this.
   */
  @Pointcut("execution(* com.moz.fiji.mapreduce.FijiMapper.cleanup(..)) ||"
      + "execution(* com.moz.fiji.mapreduce.FijiReducer.cleanup(..))")
  protected void mrCleanupPoint() {
  }

  /**
   * Logic to serialize collected profiling content to a file on HDFS. The files are stored
   * in the current working directory for this context, in a folder specified by STATS_DIR. The per
   * task file is named by the task attempt id.
   * We obtain the profiling stats collected by the LogTimerAspect in FijiSchema. The format of the
   * file is as follows: Job Name, Job ID, Task Attempt, Function Signature,
   * Aggregate Time (nanoseconds), Number of Invocations, Time per call (nanoseconds)'\n'
   *
   * @param context The {@link TaskInputOutputContext} for this job.
   * @throws IOException If the writes to HDFS fail.
   */
  private void serializeToFile(TaskInputOutputContext context) throws IOException {
    Path parentPath = new Path(context.getWorkingDirectory(), STATS_DIR);
    FileSystem fs = parentPath.getFileSystem(context.getConfiguration());
    fs.mkdirs(parentPath);
    Path path = new Path(parentPath, context.getTaskAttemptID().toString());
    OutputStreamWriter out = new OutputStreamWriter(fs.create(path, true), "UTF-8");
    try {
      out.write("Job Name, Job ID, Task Attempt, Function Signature, Aggregate Time (nanoseconds), "
          + "Number of Invocations, Time per call (nanoseconds)\n");

      ConcurrentHashMap<String, LoggingInfo> signatureTimeMap =
          mLogTimerAspect.getSignatureTimeMap();
      for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
        writeProfileInformation(out, context, entrySet.getKey(), entrySet.getValue());
      }

      signatureTimeMap = mMRLogTimerAspect.getSignatureTimeMap();
      for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
        writeProfileInformation(out, context, entrySet.getKey(), entrySet.getValue());
      }
    } finally {
      out.close();
    }
  }

  /**
   * Logic to write a profiling content for a single method signature to a file on HDFS.
   * The format of the file is as follows: Job Name, Job ID, Task Attempt, Function Signature,
   * Aggregate Time (nanoseconds), Number of Invocations, Time per call (nanoseconds)'\n'
   *
   * @param out The {@link OutputStreamWriter} for writing to the file.
   * @param context The {@link TaskInputOutputContext} for this job.
   * @param signature The method signature for the profile.
   * @param loggingInfo The profiling information for the method.
   * @throws IOException If the writes to HDFS fail.
   */
  private void writeProfileInformation(OutputStreamWriter out,
                                       TaskInputOutputContext context,
                                       String signature,
                                       LoggingInfo loggingInfo)
      throws IOException {
    // ensure that files do not end up with x.yzE7 format for floats. Instead of 1.0E3, we want
    // 1000.000
    NumberFormat nf = NumberFormat.getInstance();
    nf.setGroupingUsed(false);
    nf.setMinimumFractionDigits(1);
    nf.setMaximumFractionDigits(3);

    out.write(context.getJobName() + ", "
        + context.getJobID() + ", "
        + context.getTaskAttemptID() + ", "
        + signature + ", "
        + loggingInfo.toString() + ", "
        + nf.format(loggingInfo.perCallTime()) + "\n");
  }

  /**
   * Advice for running after any functions that match PointCut "mrCleanupPoint", such as
   * the cleanup call in some FijiMapper. This also ensures that the serialize code is
   * called only once, not in both the parent and child class. The serializeToFile function
   * serializes the profiling stats collected by the LogTimerAspect.
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   */
  @AfterReturning("mrCleanupPoint() && !cflowbelow(mrCleanupPoint())")
  public void afterMRCleanup(final JoinPoint thisJoinPoint) {
    TaskInputOutputContext context = (TaskInputOutputContext)thisJoinPoint.getArgs()[0];
    try {
      serializeToFile(context);
    } catch (IOException ioe) {
      LOG.error("Failure writing profiling results", StringUtils.stringifyException(ioe));
    }
  }
}
