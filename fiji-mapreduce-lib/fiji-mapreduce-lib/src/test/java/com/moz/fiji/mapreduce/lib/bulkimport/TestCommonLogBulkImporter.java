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

import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.TestingResources;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

/** Unit tests. */
public class TestCommonLogBulkImporter extends FijiClientTest {
  private FijiTable mTable;
  private FijiTableReader mReader;

  @Before
  public final void setupTestBulkImporter() throws Exception {
    // Get the test table layouts.
    final TableLayoutDesc tableLayoutDesc =
        FijiTableLayouts.getLayout(BulkImporterTestUtils.COMMON_LOG_LAYOUT);
    final FijiTableLayout layout = FijiTableLayout.newLayout(tableLayoutDesc);

    // Populate the environment.
    new InstanceBuilder(getFiji())
        .withTable("logs", layout)
        .build();

    // Fill local variables.
    mTable = getFiji().openTable("logs");
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestBulkImporter() throws Exception {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testCommonLogBulkImporter() throws Exception {
    // Prepare input file:
    final File inputFile = TestingResources.getResourceAsTempFile(
        BulkImporterTestUtils.COMMON_LOG_IMPORT_DATA,
        getLocalTempDir()
    );

    // Prepare descriptor file:
    final File descriptorFile = TestingResources.getResourceAsTempFile(
        BulkImporterTestUtils.COMMON_LOG_IMPORT_DESCRIPTOR,
        getLocalTempDir()
    );

    final Configuration conf = getConf();
    conf.set(DescribedInputTextBulkImporter.CONF_FILE, descriptorFile.getCanonicalPath());

    // Run the bulk-import:
    final FijiMapReduceJob job = FijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(CommonLogBulkImporter.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(3,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED).getValue());
  }
}
