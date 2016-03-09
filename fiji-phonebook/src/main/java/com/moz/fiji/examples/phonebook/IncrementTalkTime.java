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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.platform.FijiMRPlatformBridge;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.FijiURIException;
import com.moz.fiji.schema.mapreduce.DistributedCacheJars;
import com.moz.fiji.schema.mapreduce.FijiConfKeys;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Reads an input file that lists the number of minutes of talk time
 * per person. The talk time is incremented in the person's record in
 * the phone book table.
 *
 * @deprecated using "Raw" MapReduce jobs that interact with Fiji tables is no longer
 *     the preferred mechanism.  The <tt>com.moz.fiji.schema.mapreduce</tt> classes are
 *     deprecated and may be removed in a future version of FijiSchema. You should instead
 *     use the FijiMR library, extend the {@link com.moz.fiji.mapreduce.FijiMapper} class and
 *     use the {@link com.moz.fiji.mapreduce.FijiMapReduceJobBuilder} class to configure such
 *     jobs, rather than use classes like {@link DistributedCacheJars} and
 *     {@link FijiConfKeys}.
 */
public class IncrementTalkTime extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementTalkTime.class);

  /** Name of the phonebook table. */
  public static final String TABLE_NAME = "phonebook";

  /**
   * Map task that will parse phone call logs from a text file and insert the records
   * into the phonebook table, while keeping track of the total amount of talk-time
   * per contact.
   */
  public static class IncrementTalkTimeMapper
      extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private Fiji mFiji;
    private FijiTable mTable;
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
      mFiji = Fiji.Factory.open(tableURI, conf);
      mTable = mFiji.openTable(TABLE_NAME);
      mWriter = mTable.openTableWriter();
    }

    /** {@inheritDoc} */
    @Override
    public void map(LongWritable byteOffset, Text line, Context hadoopContext)
        throws IOException, InterruptedException {
      // Each line of the text file has the form:
      //
      //     firstname | lastname | talktime
      //
      // Split the input line by the pipe '|' character.
      final String[] fields = line.toString().split("\\|");

      if (3 != fields.length) {
        LOG.error("Invalid number of fields (" + fields.length + ") in line: " + line.toString());
        return; // No inserts for this mal-formed line.
      }

      // Read each line and split it into its individual components.
      final String firstName = fields[0];
      final String lastName = fields[1];
      final long talkTime = new Long(fields[2].replaceAll("\\s", ""));

      // Generate the row ID belonging to the user with this "firstname,lastname".
      final EntityId user = mTable.getEntityId(firstName + "," + lastName);

      // Add the talk time to the stats:talktime column.
      mWriter.increment(user, "stats", "talktime", talkTime);
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context hadoopContext) throws IOException, InterruptedException {
      // Safely free up resources by closing in reverse order.
      ResourceUtils.closeOrLog(mWriter);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mFiji);
      super.cleanup(hadoopContext);
    }
  }

  /**
   * Submits the IncrementTalkTimeMapper job to Hadoop.
   *
   * @param args Command line arguments; contains the path to the input text file to process.
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Fiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    // Load HBase configuration before connecting to Fiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Configure a map-only job that increments the talk time for phonebook entries.
    final Job job = new Job(getConf(), "IncrementTalkTime");

    // Read from a text file.
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    // Run the mapper that will increment talk time.
    job.setMapperClass(IncrementTalkTimeMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    // Use no reducer (this is a map-only job).
    job.setNumReduceTasks(0);
    // Since table writers do not emit any key-value pairs, we set the output format to Null.
    job.setOutputFormatClass(NullOutputFormat.class);

    // Direct the job output to the phonebook table.
    final FijiURI tableURI =
        FijiURI.newBuilder(String.format("fiji://.env/default/%s", TABLE_NAME)).build();
    job.getConfiguration().set(FijiConfKeys.OUTPUT_FIJI_TABLE_URI, tableURI.toString());

    // Tell Hadoop where the java dependencies are located, so they
    // can be shipped to the cluster during execution.
    job.setJarByClass(IncrementTalkTime.class);
    GenericTableMapReduceUtil.addAllDependencyJars(job);
    DistributedCacheJars.addJarsToDistributedCache(
        job, new File(System.getenv("FIJI_HOME"), "lib"));
    FijiMRPlatformBridge.get().setUserClassesTakesPrecedence(job, true);

    // Run the job.
    final boolean isSuccessful = job.waitForCompletion(true);

    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point. Terminates the application without returning.
   *
   * @param args Pass in the hdfs path to the phone log.
   * @throws Exception If map reduce job fails.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new IncrementTalkTime(), args));
  }
}
