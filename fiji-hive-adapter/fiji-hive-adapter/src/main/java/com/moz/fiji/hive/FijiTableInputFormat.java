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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiRegion;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * An input format that reads from Fiji Tables.
 *
 * <p>
 *   This input format exists in addition to the
 *   {@link com.moz.fiji.mapreduce.framework.FijiTableInputFormat} because we need one that is
 *   an instance of mapred.InputFormat (not mapreduce.InputFormat) for integration with hive.
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
public class FijiTableInputFormat
    implements InputFormat<ImmutableBytesWritable, FijiRowDataWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableInputFormat.class);

  public static final String CONF_FIJI_DATA_REQUEST_PREFIX = "fiji.data.request.";

  /**
   * Returns an object responsible for generating records contained in a
   * given input split.
   *
   * @param split The input split to create a record reader for.
   * @param job The job configuration.
   * @param reporter A job info reporter (for counters, status, etc.).
   * @return The record reader.
   * @throws IOException If there is an error.
   */
  @Override
  public RecordReader<ImmutableBytesWritable, FijiRowDataWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    LOG.info("Getting record reader {}", split.getLocations());
    return new FijiTableRecordReader((FijiTableInputSplit) split, job);
  }

  /**
   * Returns an array of input splits to be used as input to map tasks.
   *
   * @param job The job configuration.
   * @param numTasks A hint from the MR framework for the number of mappers.
   * @return The specifications of each split.
   * @throws IOException If there is an error.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numTasks) throws IOException {
    // TODO: Use the numTasks hint effectively. We just ignore it right now.

    final FijiURI fijiURI = getFijiURI(job);
    final InputSplit[] splits;

    Fiji fiji = null;
    FijiTable fijiTable = null;
    try {
      fiji = Fiji.Factory.open(fijiURI);
      fijiTable = fiji.openTable(fijiURI.getTable());

      // Get the start keys for each region in the table.
      List<FijiRegion> fijiRegions = fijiTable.getRegions();
      splits = new InputSplit[fijiRegions.size()];
      for (int i = 0; i < fijiRegions.size(); i++) {
        FijiRegion fijiRegion = fijiRegions.get(i);
        byte[] regionStartKey = fijiRegion.getStartKey();
        byte[] regionEndKey = fijiRegion.getEndKey();

        Collection<String> regionLocations = fijiRegion.getLocations();
        String regionHost = null;
        if (!regionLocations.isEmpty()) {
          // TODO: Allow the usage of regions that aren't the first.
          String regionLocation = regionLocations.iterator().next();
          regionHost = regionLocation.substring(0, regionLocation.indexOf(":"));
        } else {
          LOG.warn("No locations found for region: {}", fijiRegion.toString());
        }
        final Path dummyPath = FileInputFormat.getInputPaths(job)[0];
        splits[i] = new FijiTableInputSplit(fijiURI,
            regionStartKey, regionEndKey, regionHost, dummyPath);
      }
    } catch (IOException e) {
      LOG.warn("Unable to get region information.  Returning an empty list of splits.");
      LOG.warn(StringUtils.stringifyException(e));
      return new InputSplit[0];
    } finally {
      ResourceUtils.releaseOrLog(fijiTable);
      ResourceUtils.releaseOrLog(fiji);
    }
    return splits;
  }

  /**
   * Gets the name of the fiji table this input format will read from.
   *
   * @param conf The job configuration.
   * @return The name of the fiji table this input format will read from.
   */
  private static FijiURI getFijiURI(Configuration conf) {
    final String fijiURIString = conf.get(FijiTableInfo.FIJI_TABLE_URI);
    if (null == fijiURIString) {
      throw new RuntimeException("FijiTableInputFormat needs to be configured. "
          + "Please specify " + FijiTableInfo.FIJI_TABLE_URI + " in the configuration.");
    }
    FijiURI fijiURI = FijiURI.newBuilder(fijiURIString).build();
    return fijiURI;
  }
}
