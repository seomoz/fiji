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
import java.util.List;

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

import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.common.flags.FlagParser;
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
 * Deletes all entries from the phonebook table that have an address from a particular US state.
 *
 * @deprecated using "Raw" MapReduce is no longer the preferred mechanism to iterate
 *     over rows of a Fiji table. You should instead use the FijiMR library. To write a
 *     function that processes and updates a table in a row-by-row fashion, you should
 *     extend {@link com.moz.fiji.mapreduce.produce.FijiProducer}.  You should use {@link
 *     com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder} for constructing such MapReduce
 *     jobs.  You will still need to explicitly open a FijiTableWriter to call {@link
 *     FijiTableWriter#deleteRow}.
 */
public class DeleteEntriesByState extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /** Populated in the run() method with the contents of the --state command line argument. */
  @Flag(name="state", usage="This program will delete entries with this 2-letter US state code.")
  private String mState;

  /**
   * A Mapper that will be run over the rows of the phonebook table,
   * deleting the entries with an address from the US state specified
   * in the "delete.state" job configuration variable.
   */
  public static class DeleteEntriesByStateMapper
      extends Mapper<EntityId, FijiRowData, NullWritable, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntriesByStateMapper.class);

    /** Configuration variable that will contain the 2-letter US state code. */
    public static final String CONF_STATE = "delete.state";

    /** Job counters. */
    public static enum Counter {
      /** Counts the number of rows with a missing info:address column. */
      MISSING_ADDRESS
    }

    /** Fiji instance to delete from. */
    private Fiji mFiji;

    /** Fiji table to delete from. */
    private FijiTable mTable;

    /** Writer to deletes. */
    private FijiTableWriter mWriter;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context hadoopContext) throws IOException, InterruptedException {
      super.setup(hadoopContext);
      final Configuration conf = hadoopContext.getConfiguration();
      FijiURI tableURI;
      try {
        tableURI = FijiURI.newBuilder(conf.get(FijiConfKeys.OUTPUT_FIJI_TABLE_URI)).build();
      } catch (FijiURIException kue) {
        throw new IOException(kue);
      }
      mFiji = Fiji.Factory.open(tableURI, hadoopContext.getConfiguration());
      mTable = mFiji.openTable(tableURI.getTable());
      mWriter = mTable.openTableWriter();
    }

    /**
     * This method will be called once for each row of the phonebook table.
     *
     * @param entityId The entity id for the row.
     * @param row The data from the row (in this case, it would only
     *     include the address column because that is all we requested
     *     when configuring the input format).
     * @param hadoopContext The MapReduce job context used to emit output.
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

      final String victimState = hadoopContext.getConfiguration().get(CONF_STATE, "");
      final Address address = row.getMostRecentValue(Fields.INFO_FAMILY, Fields.ADDRESS);

      if (victimState.equals(address.getState().toString())) {
        // Delete the entry.
        mWriter.deleteRow(entityId);
      }
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context hadoopContext) throws IOException, InterruptedException {
      ResourceUtils.closeOrLog(mWriter);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mFiji);
      super.cleanup(hadoopContext);
    }
  }

  /**
   * Deletes all entries from the phonebook table from a specified US state.
   *
   * @param args The command line arguments (we expect a --state=XX arg here).
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Fiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    final List<String> nonFlagArgs = FlagParser.init(this, args);
    if (null == nonFlagArgs) {
      // The flags were not parsed.
      return 1;
    }

    // Load HBase configuration before connecting to Fiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Configure a map-only job that deletes entries from the specified state (mState);
    final Job job = new Job(getConf(), "DeleteEntriesByState");

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

    // Run the mapper that will delete entries from the specified state.
    job.setMapperClass(DeleteEntriesByStateMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    // Since table writers do not emit any key-value pairs, we set the output format to Null.
    job.setOutputFormatClass(NullOutputFormat.class);

    job.getConfiguration().set(DeleteEntriesByStateMapper.CONF_STATE, mState);

    // Use no reducer (this is map-only job).
    job.setNumReduceTasks(0);

    // Delete data from the Fiji phonebook table.
    job.getConfiguration().set(FijiConfKeys.OUTPUT_FIJI_TABLE_URI, tableURI.toString());

    // Tell Hadoop where the java dependencies are located, so they
    // can be shipped to the cluster during execution.
    job.setJarByClass(DeleteEntriesByState.class);
    GenericTableMapReduceUtil.addAllDependencyJars(job);
    DistributedCacheJars.addJarsToDistributedCache(
        job, new File(System.getenv("FIJI_HOME"), "lib"));
    FijiMRPlatformBridge.get().setUserClassesTakesPrecedence(job, true);

    // Run the job.
    final boolean isSuccessful = job.waitForCompletion(true);

    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point.
   *
   * @param args The command line arguments (we expect  a --state=XX here).
   * @throws Exception If there is an exception thrown from the application.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DeleteEntriesByState(), args));
  }
}
