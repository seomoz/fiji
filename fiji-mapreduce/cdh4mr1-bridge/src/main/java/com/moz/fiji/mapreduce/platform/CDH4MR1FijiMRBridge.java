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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.moz.fiji.annotations.ApiAudience;

/**
 * CDH4-backed implementation of the FijiMRPlatformBridge API.
 */
@ApiAudience.Private
public final class CDH4MR1FijiMRBridge extends FijiMRPlatformBridge {
  /** {@inheritDoc} */
  @Override
  public TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID id) {
    // In CDH4, TaskAttemptContext and its implementation are separated.
    return new TaskAttemptContextImpl(conf, id);
  }

  /** {@inheritDoc} */
  @Override
  public TaskAttemptID newTaskAttemptID(String jtIdentifier, int jobId, TaskType type,
      int taskId, int id) {
    // In CDH4, use all these args directly.
    return new TaskAttemptID(jtIdentifier, jobId, type, taskId, id);
  }

  /** {@inheritDoc} */
  @Override
  public SequenceFile.Writer newSeqFileWriter(Configuration conf, Path filename,
      Class<?> keyClass, Class<?> valueClass) throws IOException {

    Preconditions.checkArgument(conf != null, "Configuration argument must be non-null");
    Preconditions.checkArgument(filename != null, "Filename argument must be non-null");

    return SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(filename),
        SequenceFile.Writer.keyClass(keyClass),
        SequenceFile.Writer.valueClass(valueClass));
  }

  /** {@inheritDoc} */
  @Override
  public SequenceFile.Reader newSeqFileReader(Configuration conf, Path filename)
      throws IOException {

    Preconditions.checkArgument(conf != null, "Configuration argument must be non-null");
    Preconditions.checkArgument(filename != null, "Filename argument must be non-null");
    return new SequenceFile.Reader(conf, SequenceFile.Reader.file(filename));
  }


  /** {@inheritDoc} */
  @Override
  public void setUserClassesTakesPrecedence(Job job, boolean value) {
    // We can do this directly in CDH4.
    job.setUserClassesTakesPrecedence(value);
  }

  /** {@inheritDoc} */
  @Override
  public <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Context getMapperContext(
      final Configuration conf,
      final TaskAttemptID taskId,
      final RecordReader<KEYIN, VALUEIN> reader,
      final RecordWriter<KEYOUT, VALUEOUT> writer,
      final OutputCommitter committer,
      final StatusReporter reporter,
      final InputSplit split
  ) throws IOException, InterruptedException {
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext =
        new MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
            conf,
            taskId,
            reader,
            writer,
            committer,
            reporter,
            split
        );
    return new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getMapContext(mapContext);
  }

  /** {@inheritDoc} */
  @Override
  public int compareFlatKey(
      byte[] left,
      int loffset,
      int llength,
      byte[] right,
      int roffset,
      int rlength
  ) {
    return KeyValue.KEY_COMPARATOR.compare(left, loffset, llength, right, roffset, rlength);
  }

  /** {@inheritDoc} */
  @Override
  public int compareKeyValues(KeyValue left, KeyValue right) {
    return KeyValue.COMPARATOR.compare(left, right);
  }

  /** {@inheritDoc} */
  @Override
  public void writeKeyValue(KeyValue keyValue, DataOutput dataOutput) throws IOException {
    keyValue.write(dataOutput);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValue readKeyValue(DataInput dataInput) throws IOException {
    // This line is money.
    // It works around the fact that KeyValue maintains cached state by
    // just creating a fresh one before reading Writable-serialized data.
    KeyValue keyValue = new KeyValue();

    keyValue.readFields(dataInput);
    return keyValue;
  }

  /** {@inheritDoc} */
  @Override
  public void setTotalOrderPartitionerClass(Job job) {
    job.setPartitionerClass(TotalOrderPartitioner.class);
  }
}
