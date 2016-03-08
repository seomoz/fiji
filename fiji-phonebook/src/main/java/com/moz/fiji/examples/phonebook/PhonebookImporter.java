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

import java.io.IOException;
import java.util.List;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.common.flags.FlagParser;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiURI;

/**
 * Shell for the PhonebookImportMapper class.  This class manages
 * the command line arguments and job setup for the bulk importer.
 */
public class PhonebookImporter extends Configured implements Tool {
  /** Name of the table to insert phonebook entries into. */
  public static final String TABLE_NAME = "phonebook";

  /** URI of Fiji instance to use (need to support Cassandra and HBase Fiji. */
  @Flag(
      name="fiji",
      usage="Specify the Fiji instance containing the 'phonebook' table."
  )
  private String mFijiUri = "fiji://.env/default";

  @Flag(name="input-data", usage="Text file with records to import", required=true)
  private String mInputData;

  /**
   * Map task that will parse user records from a text file and insert the records
   * into the phonebook table.
   */
  public static class PhonebookBulkImporter
      extends FijiBulkImporter<LongWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(PhonebookBulkImporter.class);

    /** {@inheritDoc} */
    @Override
    public void produce(LongWritable byteOffset, Text line, FijiTableContext context)
        throws IOException {
      // Each line of the text file has the form:
      //
      //     firstname|lastname|email|telephone|addressJson
      //
      // Split the input line by the pipe '|' character.
      final String[] fields = line.toString().split("\\|");

      if (5 != fields.length) {
        LOG.error("Invalid number of fields (" + fields.length + ") in line: " + line);
        return; // No inserts for this malformed line.
      }

      // Extract the relevant fields.
      final String firstName = fields[0];
      final String lastName = fields[1];
      final String email = fields[2];
      final String telephone = fields[3];
      final String addressJson = fields[4];

      // addressJson contains a JSON-encoded Avro "Address" record. Parse this into
      // an object and write it to the person's record.
      // The Address record type is generated from src/main/avro/Address.avsc as part
      // of the build process (see avro-maven-plugin in pom.xml).
      final SpecificDatumReader<Address> datumReader =
          new SpecificDatumReader<Address>(Address.SCHEMA$);
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Address.SCHEMA$, addressJson);
      final Address streetAddr = datumReader.read(null, decoder);

      // Create a row ID with the first and last name.
      final EntityId user = context.getEntityId(firstName + "," + lastName);

      // Write the fields to appropriate table columns in the row.
      // The column names are specified as constants in the Fields.java class.
      context.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, firstName);
      context.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, lastName);
      context.put(user, Fields.INFO_FAMILY, Fields.EMAIL, email);
      context.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, telephone);
      context.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, streetAddr);
    }
  }

  /**
   * Configure the MapReduce job to run the import.
   *
   * @param inputPath the Path to the input data.
   * @param tableUri the URI to the destination table for the import.
   * @return a FijiMapReduceJob that's ready to run.
   * @throws IOException if there's an error interacting with the job or the Fiji URI.
   */
  FijiMapReduceJob configureJob(Path inputPath, FijiURI tableUri) throws IOException {
    return FijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(inputPath))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(tableUri))
        .withBulkImporter(PhonebookBulkImporter.class)
        .build();
  }

  /**
   * Submits the PhonebookImportMapper job to Hadoop.
   *
   * @param args Command line arguments; contains the path to the input text file to import.
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Fiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    // Parse command-line arguments, populating mFijiUri.
    List<String> nonFlagArgs = FlagParser.init(this, args);
    if (null == nonFlagArgs) {
      // There was a problem parsing the flags.
      return 1;
    }

    // Load HBase configuration before connecting to Fiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Direct the job output to the phonebook table. Due to the size of this data set,
    // we can write directly to the table rather than use HFileMapReduceJobOutput.
    // small amount of output
    final FijiURI tableUri = FijiURI.newBuilder(mFijiUri + "/" + TABLE_NAME).build();

    // Configure a map-only job that imports phonebook entries from a file into the table.
    final FijiMapReduceJob job = configureJob(new Path(mInputData), tableUri);

    // Run the job.
    final boolean isSuccessful = job.run();
    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point. Terminates the application without returning.
   *
   * @param args The arguments from the command line. May start with Hadoop "-D" options.
   * @throws Exception If the application encounters an exception.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PhonebookImporter(), args));
  }
}
