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

package com.moz.fiji.examples.phonebook;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.platform.FijiMRPlatformBridge;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.FijiURIException;
import com.moz.fiji.schema.mapreduce.DistributedCacheJars;
import com.moz.fiji.schema.mapreduce.FijiConfKeys;
import com.moz.fiji.schema.mapreduce.FijiTableInputFormat;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Extracts fields from the address column into individual columns in the derived column family.
 *
 * @deprecated using "Raw" MapReduce over Fiji tables is no longer the preferred
 *     mechanism for iterating over rows of a Fiji table. To write a function that
 *     processes and updates a table in a row-by-row fashion, you should extend
 *     {@link com.moz.fiji.mapreduce.produce.FijiProducer}. You should use
 *     {@link com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder} for constructing
 *     such MapReduce jobs.
 */
@Deprecated
public class AddressFieldExtractor extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /**
   * Map task that will take the Avro address field and place all the address
   * fields into individual fields in the derived column family.
   */
  public static class AddressMapper
      extends Mapper<EntityId, FijiRowData, NullWritable, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(AddressMapper.class);

    /** Job counters. */
    public static enum Counter {
      /** Counts the number of rows with a missing info:address column. */
      MISSING_ADDRESS
    }

    private Fiji mFiji;
    private FijiTable mTable;
    private FijiTableWriter mTableWriter;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context hadoopContext) throws IOException, InterruptedException {
      super.setup(hadoopContext);
      final Configuration conf = hadoopContext.getConfiguration();
      FijiURI tableURI;
      try {
        tableURI = FijiURI.newBuilder(conf.get(FijiConfKeys.OUTPUT_KIJI_TABLE_URI)).build();
      } catch (FijiURIException kue) {
        throw new IOException(kue);
      }

      mFiji = Fiji.Factory.open(tableURI, conf);
      mTable = mFiji.openTable(TABLE_NAME);
      mTableWriter = mTable.openTableWriter();
    }

    /**
     * Called once per row in the phonebook table. Extracts the Avro
     * address record into the individual fields in the derived column
     * family.
     *
     * @param entityId The id for the row.
     * @param row The row data requested (in this case, just the address column).
     * @param hadoopContext The MapReduce task context.
     * @throws IOException If there is an IO error.
     */
    @Override
    public void map(EntityId entityId, FijiRowData row, Context hadoopContext)
        throws IOException {
      // Check that the row has the info:address column.
      // The column names are specified as constants in the Fields.java class.
      if (!row.containsColumn(Fields.INFO_FAMILY, Fields.ADDRESS)) {
        LOG.info("Missing address field in row: " + entityId);
        hadoopContext.getCounter(Counter.MISSING_ADDRESS).increment(1L);
        return;
      }
      final Address address = row.getMostRecentValue(Fields.INFO_FAMILY, Fields.ADDRESS);

      // Write the data in the address record into individual columns.
      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ADDR_LINE_1, address.getAddr1());

      // Optional.
      if (null != address.getApt()) {
        mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.APT_NUMBER, address.getApt());
      }

      // Optional.
      if (null != address.getAddr2()) {
        mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ADDR_LINE_2, address.getAddr2());
      }

      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.CITY, address.getCity());
      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.STATE, address.getState());
      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ZIP, address.getZip());
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context hadoopContext) throws IOException, InterruptedException {
      ResourceUtils.closeOrLog(mTableWriter);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mFiji);
      super.cleanup(hadoopContext);
    }
  }

  /**
   * Submits the AddressMapper job to Hadoop.
   *
   * @param args Command line arguments; none expected.
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Fiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    // Load HBase configuration before connecting to Fiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Configure a map-only job that extracts address records into the individual fields.
    final Job job = new Job(getConf(), "AddressFieldExtractor");

    // Read from the Fiji phonebook table.
    job.setInputFormatClass(FijiTableInputFormat.class);
    final FijiDataRequest dataRequest = FijiDataRequest.create(Fields.INFO_FAMILY, Fields.ADDRESS);
    final FijiURI tableURI =
        FijiURI.newBuilder(String.format("fiji://.env/default/%s", TABLE_NAME)).build();
    FijiTableInputFormat.configureJob(
        job,
        tableURI,
        dataRequest,
        /* start row */ null,
        /* end row */ null);

    // Run the mapper that will do the address extraction.
    job.setMapperClass(AddressMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    // Since table writers do not emit any key-value pairs, we set the output format to Null.
    job.setOutputFormatClass(NullOutputFormat.class);

    // Use no reducer (this is map-only job).
    job.setNumReduceTasks(0);

    // Write extracted data to the Fiji phonebook table.
    job.getConfiguration().set(FijiConfKeys.OUTPUT_KIJI_TABLE_URI, tableURI.toString());

    // Tell Hadoop where the java dependencies are located, so they
    // can be shipped to the cluster during execution.
    job.setJarByClass(AddressMapper.class);
    GenericTableMapReduceUtil.addAllDependencyJars(job);
    DistributedCacheJars.addJarsToDistributedCache(job,
        new File(System.getenv("KIJI_HOME"), "lib"));
    FijiMRPlatformBridge.get().setUserClassesTakesPrecedence(job, true);

    // Run the job.
    final boolean isSuccessful = job.waitForCompletion(true);

    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point.
   *
   * @param args Arguments to AddressFieldExtractor job
   * @throws Exception General main exceptions
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new AddressFieldExtractor(), args));
  }
}
