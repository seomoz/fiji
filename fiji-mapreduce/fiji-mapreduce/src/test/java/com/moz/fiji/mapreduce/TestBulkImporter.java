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

package com.moz.fiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

/** Runs a bulk-importer job in-process against a fake HBase instance. */
public class TestBulkImporter extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestBulkImporter.class);

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
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
  }

  /**
   * Producer intended to run on the generic FijiMR test layout. Uses the resource
   * com.moz.fiji/mapreduce/layout/test.json
   */
  public static class SimpleBulkImporter extends FijiBulkImporter<LongWritable, Text> {
    /** {@inheritDoc} */
    @Override
    public void produce(LongWritable inputKey, Text value, FijiTableContext context)
        throws IOException {
      final String line = value.toString();
      final String[] split = line.split(":");
      Preconditions.checkState(split.length == 2,
          String.format("Unable to parse bulk-import test input line: '%s'.", line));
      final String rowKey = split[0];
      final String name = split[1];

      final EntityId eid = context.getEntityId(rowKey);
      context.put(eid, "primitives", "string", name);
      context.put(eid, "primitives", "long", inputKey.get());
    }
  }

  @Test
  public void testSimpleBulkImporter() throws Exception {
    // Prepare input file:
    final File inputFile = File.createTempFile("TestBulkImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get("com.moz.fiji/mapreduce/TestBulkImportInput.txt"));

    // Run the bulk-import:
    final FijiMapReduceJob job = FijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withBulkImporter(SimpleBulkImporter.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate output:
    final FijiRowScanner scanner = mReader.getScanner(FijiDataRequest.create("primitives"));
    for (FijiRowData row : scanner) {
      final EntityId eid = row.getEntityId();
      final String rowId = Bytes.toString((byte[]) eid.getComponentByIndex(0));
      final String cellContent = row.getMostRecentValue("primitives", "string").toString();
      LOG.info("Row: {}, primitives.string: {}, primitives.long: {}",
          rowId, cellContent, row.getMostRecentValue("primitives", "long"));
      if (rowId.equals("row1")) {
        assertEquals("Marsellus Wallace", cellContent);
      } else if (rowId.equals("row2")) {
        assertEquals("Vincent Vega", cellContent);
      } else {
        fail();
      }
    }
    scanner.close();
  }

  /**
   * Producer intended to run on the generic FijiMR test layout.
   *
   * @see testing resource com.moz.fiji/mapreduce/layout/test.json
   */
  public static class BulkImporterWorkflow extends FijiBulkImporter<LongWritable, Text> {
    private boolean mSetupFlag = false;
    private int mProduceCounter = 0;
    private boolean mCleanupFlag = false;

    /** {@inheritDoc} */
    @Override
    public void setup(FijiTableContext context) throws IOException {
      super.setup(context);
      assertFalse(mSetupFlag);
      assertEquals(0, mProduceCounter);
      mSetupFlag = true;
    }

    /** {@inheritDoc} */
    @Override
    public void produce(LongWritable inputKey, Text value, FijiTableContext context)
        throws IOException {
      assertTrue(mSetupFlag);
      assertFalse(mCleanupFlag);
      mProduceCounter += 1;

      final String line = value.toString();
      final String[] split = line.split(":");
      Preconditions.checkState(split.length == 2,
          String.format("Unable to parse bulk-import test input line: '%s'.", line));
      final String rowKey = split[0];
      final String name = split[1];

      final EntityId eid = context.getEntityId(rowKey);
      context.put(eid, "primitives", "string", name);
    }

    /** {@inheritDoc} */
    @Override
    public void cleanup(FijiTableContext context) throws IOException {
      assertTrue(mSetupFlag);
      assertFalse(mCleanupFlag);
      assertEquals(2, mProduceCounter);  // input file has 2 entries
      mCleanupFlag = true;
      super.cleanup(context);
    }
  }

  /** Tests the bulk-importer workflow (setup/produce/cleanup) and counters. */
  @Test
  public void testBulkImporterWorkflow() throws Exception {
    // Prepare input file:
    final File inputFile = File.createTempFile("TestBulkImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get("com.moz.fiji/mapreduce/TestBulkImportInput.txt"));

    // Run the bulk-import:
    final FijiMapReduceJob job = FijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withBulkImporter(BulkImporterWorkflow.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate output:
    final FijiRowScanner scanner = mReader.getScanner(FijiDataRequest.create("primitives"));
    final Set<String> produced = Sets.newHashSet();
    for (FijiRowData row : scanner) {
      final String string = row.getMostRecentValue("primitives", "string").toString();
      produced.add(string);
    }
    scanner.close();

    assertTrue(produced.contains("Marsellus Wallace"));
    assertTrue(produced.contains("Vincent Vega"));

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(2,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
  }

  // TODO(KIJI-359): Implement missing tests
  //  - bulk-importing to HFiles
  //  - bulk-importing multiple files
}
