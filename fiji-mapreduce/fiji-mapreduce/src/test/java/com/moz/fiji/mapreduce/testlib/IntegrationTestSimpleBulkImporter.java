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

package com.moz.fiji.mapreduce.testlib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.HFileLoader;
import com.moz.fiji.mapreduce.FijiMRTestLayouts;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;

/** Tests bulk-importers. */
public class IntegrationTestSimpleBulkImporter extends AbstractFijiIntegrationTest {
  private Configuration mConf = null;
  private FileSystem mFS = null;
  private Path mBulkImportInputPath = null;
  private Fiji mFiji = null;
  private FijiTable mOutputTable = null;

  /**
   * Generates a random HDFS path.
   *
   * @param prefix Prefix for the random file name.
   * @return a random HDFS path.
   * @throws Exception on error.
   */
  private Path makeRandomPath(String prefix) throws Exception {
    Preconditions.checkNotNull(mFS);
    final Path base = new Path(FileSystem.getDefaultUri(mConf));
    final Random random = new Random(System.nanoTime());
    return new Path(base, String.format("/%s-%s", prefix, random.nextLong()));
  }

  private void writeBulkImportInput(Path path) throws Exception {
    final String[] inputLines = {
        "row1:1",
        "row2:2",
        "row3:2",
        "row4:2",
        "row5:5",
        "row6:1",
        "row7:2",
        "row8:1",
        "row9:2",
        "row10:2",
    };

    final OutputStream ostream = mFS.create(path);
    for (String line : inputLines) {
      IOUtils.write(line, ostream);
      ostream.write('\n');
    }
    ostream.close();
  }

  /**
   * Reads a table into a map from Fiji row keys to FijiRowData.
   *
   * @param table Fiji table to read from.
   * @param kdr Fiji data request.
   * @return a map of the rows.
   * @throws Exception on error.
   */
  private static Map<String, FijiRowData> toRowMap(FijiTable table, FijiDataRequest kdr)
      throws Exception {
    final FijiTableReader reader = table.openTableReader();
    try {
      final FijiRowScanner scanner = reader.getScanner(kdr);
      try {
        final Map<String, FijiRowData> rows = Maps.newHashMap();
        for (FijiRowData row : scanner) {
          rows.put(Bytes.toString((byte[]) row.getEntityId().getComponentByIndex(0)), row);
        }
        return rows;
      } finally {
        scanner.close();
      }
    } finally {
      reader.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    mConf = createConfiguration();
    mFS = FileSystem.get(mConf);
    mBulkImportInputPath = makeRandomPath("bulk-import-input");
    writeBulkImportInput(mBulkImportInputPath);
    mFiji = Fiji.Factory.open(getFijiURI(), mConf);
    final FijiTableLayout layout = FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());
    mFiji.createTable("test", layout);
    mOutputTable = mFiji.openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    mOutputTable.release();
    mFiji.release();
    mFS.delete(mBulkImportInputPath, false);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973

    mOutputTable = null;
    mFiji = null;
    mBulkImportInputPath = null;
    mFS = null;
    mConf = null;
  }

  @Test
  public void testSimpleBulkImporterDirect() throws Exception {
    final FijiMapReduceJob mrjob = FijiBulkImportJobBuilder.create()
        .withConf(mConf)
        .withBulkImporter(SimpleBulkImporter.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mOutputTable.getURI()))
        .build();
    assertTrue(mrjob.run());

    validateOutputTable();
  }

  @Test
  public void testSimpleBulkImporterHFile() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    try {
      final FijiMapReduceJob mrjob = FijiBulkImportJobBuilder.create()
          .withConf(mConf)
          .withBulkImporter(SimpleBulkImporter.class)
          .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
          .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
              mOutputTable.getURI(), hfileDirPath))
          .build();
      assertTrue(mrjob.run());

      final HFileLoader loader = HFileLoader.create(mConf);
      // There is only one reducer, hence one HFile shard:
      final Path hfilePath = new Path(hfileDirPath, "part-r-00000.hfile");
      loader.load(hfilePath, mOutputTable);

      validateOutputTable();

    } finally {
      mFS.delete(hfileDirPath, true);
    }
  }

  private void validateOutputTable() throws Exception {
    final FijiDataRequest kdr = FijiDataRequest.create("primitives");
    final Map<String, FijiRowData> rows = toRowMap(mOutputTable, kdr);
    assertEquals(10, rows.size());
    for (int i = 1; i <= 10; ++i) {
      final String rowId = String.format("row%d", i);
      assertTrue(rows.containsKey(rowId));
      assertTrue(
          rows.get(rowId).getMostRecentValue("primitives", "string").toString()
              .startsWith(rowId + "-"));
    }
  }

  // TODO: tests with # of splits > 1.

}
