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

package com.moz.fiji.mapreduce.platform;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.delegation.Lookups;

/**
 * Abstract representation of an underlying platform for FijiMR. This interface
 * is fulfilled by specific implementation providers that are dynamically chosen
 * at runtime based on the Hadoop &amp; HBase jars available on the classpath.
 */
@ApiAudience.Framework
public abstract class FijiMRPlatformBridge {
  /**
   * This API should only be implemented by other modules within FijiMR;
   * to discourage external users from extending this class, keep the c'tor
   * package-private.
   */
  FijiMRPlatformBridge() {
  }

  /**
   * Create and return a new TaskAttemptContext implementation parameterized by the specified
   * configuration and task attempt ID objects.
   *
   * @param conf the Configuration to use for the task attempt.
   * @param id the TaskAttemptID of the task attempt.
   * @return a new TaskAttemptContext.
   */
  public abstract TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID id);

  /**
   * Create and return a new TaskAttemptID object.
   *
   * @param jtIdentifier the jobtracker id.
   * @param jobId the job id number.
   * @param type the type of the task being created.
   * @param taskId the task id number within this job.
   * @param id the id number of the attempt within this task.
   * @return a newly-constructed TaskAttemptID.
   */
  public abstract TaskAttemptID newTaskAttemptID(String jtIdentifier, int jobId, TaskType type,
      int taskId, int id);


  /**
   * Create and return a new SequenceFile.Writer object.
   *
   * @param conf the current Configuration.
   * @param filename the file to open for write access.
   * @param keyClass the class representing the 'key' data type in the key-value pairs to write.
   * @param valueClass the class representing the 'value' data type in the key-value pairs to write.
   * @return a new SequenceFile.Writer object opened and ready to write to the file.
   * @throws IOException if there is an error opening the file.
   */
  public abstract SequenceFile.Writer newSeqFileWriter(Configuration conf, Path filename,
      Class<?> keyClass, Class<?> valueClass) throws IOException;


  /**
   * Create and return a new SequenceFile.Reader object.
   *
   * @param conf the current Configuration
   * @param filename the file to open for read access.
   * @return a new SequenceFile.Reader object opened and ready to read the file.
   * @throws IOException if there is an error opening the file.
   */
  public abstract SequenceFile.Reader newSeqFileReader(Configuration conf, Path filename)
      throws IOException;

  /**
   * Set the boolean property of a given Job for specifying which classpath takes precedence, the
   * user's or the system's, when tasks are launched.
   *
   * @param job the Job for which to set the property.
   * @param value the value to which to set the property.
   */
  public abstract void setUserClassesTakesPrecedence(Job job, boolean value);

  /**
   * Get a new Mapper.Context.
   *
   * @param conf the Hadoop Configuration used to configure the Context.
   * @param taskId the TaskAttemptID for the Context.
   * @param reader the RecordReader for the Context.
   * @param writer the RecordWriter for the Context.
   * @param committer the OutputCommit for the Context.
   * @param reporter the StatusReporter for the Context.
   * @param split the InputSplit for the Context.
   * @param <KEYIN> the type of the RecordReader key.
   * @param <VALUEIN> the type of the RecordReader value.
   * @param <KEYOUT> the type of the RecordWriter key.
   * @param <VALUEOUT> the type of the RecordWriter value.
   * @return a new Mapper.Context object.
   * @throws IOException in case of an IO error.
   * @throws InterruptedException in case of an interruption.
   */
  public abstract <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Mapper.Context getMapperContext(
      Configuration conf,
      TaskAttemptID taskId,
      RecordReader<KEYIN, VALUEIN> reader,
      RecordWriter<KEYOUT, VALUEOUT> writer,
      OutputCommitter committer,
      StatusReporter reporter,
      InputSplit split
  ) throws IOException, InterruptedException;

  /**
   * Compares the keys from two KeyValues, assuming the KeyValues are laid out in byte arrays.
   *
   * @param left The byte[] that holds the left KeyValue.
   * @param loffset Where to start comparing from in the left byte[].
   * @param llength The length of the left byte[] to compare.
   * @param right The byte[] that holds the right KeyValue.
   * @param roffset Where to start comparing from in the right byte[].
   * @param rlength The length of the right byte[] to compare.
   * @return The result of comparing the keys from the KeyValues.
   */
  public abstract int compareFlatKey(
      byte[] left,
      int loffset,
      int llength,
      byte[] right,
      int roffset,
      int rlength
      );

  /**
   * Compares two KeyValues, according to their keys only.
   *
   * Follows the convention of returning a negative integer, zero, or a positive integer as the
   * left argument is less than, equal to, or greater than the right argument.
   *
   * @param left The left KeyValue to compare.
   * @param right The right KeyValue to compare.
   * @return A negative integer, zero, or a positive integer as the left is less than, equal to,
   *     or greater than the right.
   */
  public abstract int compareKeyValues(final KeyValue left, final KeyValue right);

  /**
   * Writes the keyValue object to the dataOutput.
   *
   * @param keyValue The KeyValue to write.
   * @param dataOutput The DataOutput to write to.
   * @throws java.io.IOException If an I/O error occurred while writing.
   */
  public abstract void writeKeyValue(final KeyValue keyValue, DataOutput dataOutput) throws
      IOException;

  /**
   * Reads a KeyValue object from dataInput.
   *
   * This works around the fact that KeyValue maintains cached state by
   * just creating a fresh one before reading Writable-serialized data.
   *
   * @param dataInput The DataInput to read from.
   * @return A new KeyValue object, where the fields are read from DataInput.
   * @throws IOException If an I/O error occurred while reading.
   */
  public abstract KeyValue readKeyValue(DataInput dataInput) throws IOException;

  /**
   * Sets the PartitionerClass on the job to the TotalOrderPartitioner Class,
   * which has moved in some versions.
   *
   * @param job The job to set the PartitionerClass to TotalOrderPartitioner.class.
   */
  public abstract void setTotalOrderPartitionerClass(Job job);

  private static FijiMRPlatformBridge mBridge;

  /**
   * @return the FijiMRPlatformBridge implementation appropriate to the current runtime
   * conditions.
   */
  public static final synchronized FijiMRPlatformBridge get() {
    if (null != mBridge) {
      return mBridge;
    }
    mBridge = Lookups.getPriority(FijiMRPlatformBridgeFactory.class).lookup().getBridge();
    return mBridge;
  }
}

