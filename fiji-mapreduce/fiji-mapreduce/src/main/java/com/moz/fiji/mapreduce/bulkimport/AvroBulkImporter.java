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

package com.moz.fiji.mapreduce.bulkimport;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.avro.AvroKeyReader;

/**
 * Base class for Fiji bulk importers that process Avro container
 * files.  You may extend this class to be used as the --importer flag
 * when you have specified an --input flag of
 * <code>avro:&lt;filename&gt;</code> in your <code>fiji
 * bulk-import</code> command.
 *
 * @param <T> The type of the Avro data to be processed.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
public abstract class AvroBulkImporter<T> extends FijiBulkImporter<AvroKey<T>, NullWritable>
    implements AvroKeyReader {

  @Override
  public final void produce(AvroKey<T> key, NullWritable ignore,
      FijiTableContext context)
      throws IOException {
    produce(key.datum(), context);
  }

  /**
   * Process an Avro datum from the input container file to produce
   * Fiji output.
   *
   * @param datum An Avro datum from the input file.
   * @param context A context which can be used to write Fiji data.
   *     data should not be written for this input record.
   * @throws IOException If there is an error.
   */
  protected abstract void produce(T datum, FijiTableContext context)
      throws IOException;

  /**
   * Specifies the expected reader schema for the input data. By default, this
   * returns null, meaning that it will accept as input any writer schema
   * encountered in the input. Clients expecting homogenous input elements
   * (e.g., those using Avro specific record classes) should specify their
   * schema here.
   *
   * @throws IOException If there is an error determining the schema.
   * @return The input avro schema.
   */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return null;
  }
}
