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

package com.moz.fiji.mapreduce.lib.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;

/**
 * A program that runs the {@link com.moz.fiji.mapreduce.lib.examples.EmailDomainProducer}
 * over a Fiji table.
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$KIJI_HOME/bin/fiji classpath` \
 * &gt;   com.moz.fiji.mapreduce.lib.examples.EmailDomainProduceJob \
 * &gt;   instance-name table-name
 * </pre>
 */
public class EmailDomainProduceJob extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(EmailDomainProduceJob.class);

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    if (2 != args.length) {
      throw new IllegalArgumentException(
          "Invalid number of arguments. Requires fiji-instance-name and table-name.");
    }

    // Read the arguments from the command-line.
    String instanceName = args[0];
    String fijiTableName = args[1];
    LOG.info("Configuring a produce job over table " + fijiTableName + ".");

    LOG.info("Loading HBase configuration...");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    LOG.info("Opening a fiji connection...");
    FijiURI fijiURI = FijiURI.newBuilder().withInstanceName(instanceName).build();
    Fiji fiji = Fiji.Factory.open(fijiURI, getConf());

    LOG.info("Opening fiji table " + fijiTableName + "...");
    FijiTable table = fiji.openTable(fijiTableName);

    LOG.info("Configuring a produce job...");
    FijiProduceJobBuilder jobBuilder = FijiProduceJobBuilder.create()
        .withInputTable(table.getURI())
        .withProducer(EmailDomainProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(table.getURI()));

    LOG.info("Building the produce job...");
    FijiMapReduceJob job = jobBuilder.build();

    LOG.info("Running the job...");
    boolean isSuccessful = job.run();

    table.release();
    fiji.release();

    LOG.info(isSuccessful ? "Job succeeded." : "Job failed.");
    return isSuccessful ? 0 : 1;
  }

  /**
   * The program's entry point.
   *
   * <pre>
   * USAGE:
   *
   *     EmailDomainProduceJob &lt;fiji-instance&gt; &lt;fiji-table&gt;
   *
   * ARGUMENTS:
   *
   *     fiji-instance: Name of the fiji instance the table is in.
   *
   *     fiji-table: Name of the fiji table to produce over.
   * </pre>
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new EmailDomainProduceJob(), args));
  }
}
