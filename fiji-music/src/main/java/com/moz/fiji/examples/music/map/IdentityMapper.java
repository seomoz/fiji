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

package com.moz.fiji.examples.music.map;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

import com.moz.fiji.examples.music.SongCount;
import com.moz.fiji.mapreduce.FijiMapper;
import com.moz.fiji.mapreduce.avro.AvroKeyReader;
import com.moz.fiji.mapreduce.avro.AvroKeyWriter;
import com.moz.fiji.mapreduce.avro.AvroValueReader;
import com.moz.fiji.mapreduce.avro.AvroValueWriter;

/**
 * This mapper emits the same key-value pairs that are passed in.
 *
 * It is used when we run a mapreduce where the map phase is irrelevant.
 */
public class IdentityMapper
    extends FijiMapper<
        AvroKey<CharSequence>, AvroValue<SongCount>, AvroKey<CharSequence>, AvroValue<SongCount>>
    implements AvroKeyWriter, AvroValueWriter, AvroKeyReader, AvroValueReader {

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }

   /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return Schema.create(Schema.Type.STRING);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    return Schema.create(Schema.Type.STRING);
  }

  /** {@inheritDoc} */
  @Override
  public void map(AvroKey<CharSequence> key, AvroValue<SongCount> value, Context context)
    throws IOException, InterruptedException {
    context.write(key, value);
  }
}
