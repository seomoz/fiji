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

package com.moz.fiji.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.schema.FijiURI;

/**
 * An output format that writes to Fiji Tables.
 *
 * <p>
 *   This output format exists in addition to the
 *   {@link com.moz.fiji.mapreduce.framework.FijiTableOutputFormat} because we need one that is
 *   an instance of mapred.InputFormat (not mapreduce.FijiTableOutputFormat) for integration with
 *   Hive.
 * </p>
 *
 * <p>
 *   The hook that hive provides for turning MapReduce records into rows of a 2-dimensional
 *   SQL-like table is called a Deserializer. Unfortunately, Deserializers only have access to
 *   the value of the record (not the key). This means that even though this input format
 *   generates ImmutableBytesWritable keys that contain the row key of the input fiji table, the
 *   Deserializer won't have access to it. Hence, all of the data required to generate the
 *   2-dimensional view of the row must be contained in the value (in this case, the HBase Result).
 * </p>
 */
public class FijiTableOutputFormat
    implements HiveOutputFormat<ImmutableBytesWritable, FijiRowDataWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableOutputFormat.class);

  public static final String CONF_KIJI_DATA_REQUEST = "fiji.data.request";
  public static final String CONF_KIJI_TABLE_URI = "fiji.table.uri";

  /**
   * Gets the name of the fiji table this input format will read from.
   *
   * @param conf The job configuration.
   * @return The name of the fiji table this input format will read from.
   */
  private static FijiURI getFijiURI(Configuration conf) {
    final String fijiURIString = conf.get(FijiTableInfo.KIJI_TABLE_URI);
    if (null == fijiURIString) {
      throw new RuntimeException("FijiTableOutputFormat needs to be configured. "
          + "Please specify " + FijiTableInfo.KIJI_TABLE_URI + " in the configuration.");
    }
    FijiURI fijiURI = FijiURI.newBuilder(fijiURIString).build();
    return fijiURI;
  }

  @Override
  public RecordWriter<ImmutableBytesWritable, FijiRowDataWritable> getRecordWriter(
      FileSystem fileSystem, JobConf entries, String s, Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException("Hive should not be calling getRecordWriter()");
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {}

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
                                                           Path finalOutPath,
                                                           Class<? extends Writable> valueClass,
                                                           boolean isCompressed,
                                                           Properties tableProperties,
                                                           Progressable progress)
      throws IOException {
    return new FijiTableRecordWriter(jc);
  }
}
