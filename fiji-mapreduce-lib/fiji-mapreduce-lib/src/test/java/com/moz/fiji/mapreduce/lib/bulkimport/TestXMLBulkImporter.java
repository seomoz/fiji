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

package com.moz.fiji.mapreduce.lib.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.FijiMRTestLayouts;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.TestingResources;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.input.impl.XMLInputFormat;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestXMLBulkImporter extends FijiClientTest {
  private FijiTable mTable;
  private FijiTableReader mReader;

  @Before
  public final void setupTestBulkImporter() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());

    // Populate the environment.
    new InstanceBuilder(getFiji())
        .withTable("test", layout)
        .build();

    // Fill local variables.
    mTable = getFiji().openTable("test");
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestBulkImporter() throws Exception {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testXMLBulkImporter() throws Exception {
    // Prepare input file:
    final File inputFile = TestingResources.getResourceAsTempFile(
        BulkImporterTestUtils.XML_IMPORT_DATA,
        getLocalTempDir()
    );

    // Prepare descriptor file:
    final File descriptorFile = TestingResources.getResourceAsTempFile(
        BulkImporterTestUtils.FOO_XML_IMPORT_DESCRIPTOR,
        getLocalTempDir()
    );

    final Configuration conf = getConf();
    conf.set(DescribedInputTextBulkImporter.CONF_FILE, descriptorFile.getCanonicalPath());
    conf.set(XMLInputFormat.RECORD_TAG_CONF_KEY, "user");

    // Run the bulk-import:
    final FijiMapReduceJob job = FijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(XMLBulkImporter.class)
        .withInput(MapReduceJobInputs.newXMLMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Confirm expected counter values:
    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(3,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
    assertEquals(1,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED).getValue());

    // Validate output:
    final FijiRowScanner scanner = mReader.getScanner(FijiDataRequest.create("info"));
    try {
      BulkImporterTestUtils.validateImportedRows(scanner, false);
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testTimestampXMLBulkImporter() throws Exception {
    // Prepare input file:
    final File inputFile = TestingResources.getResourceAsTempFile(
        BulkImporterTestUtils.XML_IMPORT_DATA,
        getLocalTempDir()
    );

    // Prepare descriptor file:
    final File descriptorFile = TestingResources.getResourceAsTempFile(
        BulkImporterTestUtils.FOO_TIMESTAMP_XML_IMPORT_DESCRIPTOR,
        getLocalTempDir()
    );

    final Configuration conf = getConf();
    conf.set(DescribedInputTextBulkImporter.CONF_FILE, descriptorFile.getCanonicalPath());
    conf.set(XMLInputFormat.RECORD_TAG_CONF_KEY, "user");

    // Run the bulk-import:
    final FijiMapReduceJob job = FijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(XMLBulkImporter.class)
        .withInput(MapReduceJobInputs.newXMLMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Confirm expected counter values:
    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(3,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
    assertEquals(1,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED).getValue());

    // Validate output:
    final FijiRowScanner scanner = mReader.getScanner(FijiDataRequest.create("info"));
    try {
      BulkImporterTestUtils.validateImportedRows(scanner, true);
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testMultipleLocalityGroup() throws Exception {
    //Setup special table:
    getFiji().createTable(FijiTableLayouts.getLayout(FijiMRTestLayouts.LG_TEST_LAYOUT));
    final FijiTable table = getFiji().openTable("testlg");
    try {
      // Prepare input file:
      final File inputFile = TestingResources.getResourceAsTempFile(
          BulkImporterTestUtils.XML_IMPORT_DATA,
          getLocalTempDir()
      );

      // Prepare descriptor file:
      final File descriptorFile = TestingResources.getResourceAsTempFile(
          BulkImporterTestUtils.FOO_LG_XML_IMPORT_DESCRIPTOR,
          getLocalTempDir()
      );

      final Configuration conf = new Configuration(getConf());
      conf.set(DescribedInputTextBulkImporter.CONF_FILE, descriptorFile.getCanonicalPath());
      conf.set(XMLInputFormat.RECORD_TAG_CONF_KEY, "user");

      // Run the bulk-import:
      final FijiMapReduceJob job = FijiBulkImportJobBuilder.create()
          .withConf(conf)
          .withBulkImporter(XMLBulkImporter.class)
          .withInput(MapReduceJobInputs.newXMLMapReduceJobInput(new Path(inputFile.toString())))
          .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(table.getURI()))
          .build();
      assertTrue(job.run());

      // Confirm data was written correctly:
      final FijiTableReader reader = table.openTableReader();
      try {
        final FijiDataRequest request = FijiDataRequest.create("info", "name");
        final FijiDataRequest request2 = FijiDataRequest.create("other", "email");
        assertEquals(
            reader
                .get(table.getEntityId("John"), request)
                .getMostRecentCell("info", "name")
                .getData().toString(),
            "John");
        assertEquals(
            reader
                .get(table.getEntityId("John"), request2)
                .getMostRecentCell("other", "email")
                .getData().toString(),
            "johndoe@gmail.com");
        assertEquals(
            reader
                .get(table.getEntityId("Alice"), request)
                .getMostRecentCell("info", "name")
                .getData().toString(),
            "Alice");
        assertEquals(
            reader
                .get(table.getEntityId("Alice"), request2)
                .getMostRecentCell("other", "email")
                .getData().toString(),
            "alice.smith@yahoo.com");
        assertEquals(
            reader
                .get(table.getEntityId("Bob"), request)
                .getMostRecentCell("info", "name")
                .getData().toString(),
            "Bob");
        assertEquals(
            reader
                .get(table.getEntityId("Bob"), request2)
                .getMostRecentCell("other", "email")
                .getData().toString(),
            "bobbyj@aol.com");
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
