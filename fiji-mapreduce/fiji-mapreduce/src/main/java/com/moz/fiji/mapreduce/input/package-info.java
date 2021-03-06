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

/**
 * MapReduce job input types.
 *
 * <p>
 *   {@link com.moz.fiji.mapreduce.MapReduceJobInput} is the base type for classes that can act
 *   as inputs to a MapReduce job for jobs that are created using a
 *   {@link com.moz.fiji.mapreduce.framework.MapReduceJobBuilder}.
 * </p>
 *
 * <h2>Usable FijiMR input types</h2>
 * <ul>
 * <li>{@link com.moz.fiji.mapreduce.input.AvroKeyMapReduceJobInput} - Avro container files containing
 *     keys as input.</li>
 * <li>{@link com.moz.fiji.mapreduce.input.AvroKeyValueMapReduceJobInput} - Avro container files
 *     containing key value pairs as input.</li>
 * <li>{@link com.moz.fiji.mapreduce.input.HTableMapReduceJobInput} - HBase table as input.</li>
 * <li>{@link com.moz.fiji.mapreduce.input.FijiTableMapReduceJobInput} - Fiji table as input.</li>
 * <li>{@link com.moz.fiji.mapreduce.input.SequenceFileMapReduceJobInput} - Hadoop sequence file as
 *     input.</li>
 * <li>{@link com.moz.fiji.mapreduce.input.TextMapReduceJobInput} - text files in HDFS as input with
 *     each line as a row.</li>
 * <li>{@link com.moz.fiji.mapreduce.input.WholeTextFileMapReduceJobInput} - text files in HDFS as
 *     input with each file as a row.</li>
 * </ul>
 *
 * @see com.moz.fiji.mapreduce.framework.MapReduceJobBuilder
 */
package com.moz.fiji.mapreduce.input;
