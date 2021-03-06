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

package com.moz.fiji.mapreduce.lib.reduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import com.moz.fiji.mapreduce.FijiReducer;
import com.moz.fiji.mapreduce.avro.AvroKeyWriter;

/**
 * Base class for reducers used with AvroOutputFormat to write Avro container files.
 *
 * @param <K> The type of the MapReduce reducer input key.
 * @param <V> The type of the MapReduce reducer input value.
 * @param <T> The Avro type of the messages to output to the Avro container files.
 */
public abstract class AvroReducer<K, V, T> extends FijiReducer<K, V, AvroKey<T>, NullWritable>
    implements AvroKeyWriter {
  /** A shared AvroKey wrapper that is reused when writing MapReduce output keys. */
  private AvroKey<T> mKey;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    mKey = new AvroKey<T>(null);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }

  /**
   * Subclasses can use this instead of context.write() to output Avro
   * messages directly instead of having to wrap them in AvroKey
   * container objects.
   *
   * @param value The avro value to write.
   * @param context The reducer context.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  protected void write(T value, Context context) throws IOException, InterruptedException {
    mKey.datum(value);
    context.write(mKey, NullWritable.get());
  }
}
