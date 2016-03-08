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

import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.util.Random;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.HFileLoader;
import com.moz.fiji.mapreduce.FijiMRTestLayouts;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.TestingResources;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;

/** Tests bulk-importers. */
public class IntegrationTestCSVBulkImporter
    extends AbstractFijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestCSVBulkImporter.class);

  private Configuration mConf = null;
  private FileSystem mFS = null;
  private Path mBulkImportInputPath = null;
  private Path mImportDescriptorPath = null;
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

  private void writeTestResource(Path path, String testResource) throws Exception {
    final OutputStream ostream = mFS.create(path);
    IOUtils.write(TestingResources.get(testResource), ostream);
    ostream.close();
  }

  @Before
  public void setUp() throws Exception {
    mConf = createConfiguration();
    mFS = FileSystem.get(mConf);
    mBulkImportInputPath = makeRandomPath("bulk-import-input");
    writeTestResource(mBulkImportInputPath, BulkImporterTestUtils.TIMESTAMP_CSV_IMPORT_DATA);

    mImportDescriptorPath = makeRandomPath("import-descriptor");
    writeTestResource(mImportDescriptorPath, BulkImporterTestUtils.FOO_TIMESTAMP_IMPORT_DESCRIPTOR);

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

    mOutputTable = null;
    mFiji = null;
    mBulkImportInputPath = null;
    mFS = null;
    mConf = null;
  }

  // There may be a bug introduces by some changes in FijiMR which break this test.
  // @Test
  public void testCSVBulkImporterHFile() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    try {
      mConf.set(DescribedInputTextBulkImporter.CONF_FILE, mImportDescriptorPath.toString());

      final FijiMapReduceJob mrjob = FijiBulkImportJobBuilder.create()
          .withConf(mConf)
          .withBulkImporter(CSVBulkImporter.class)
          .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
          .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
              mOutputTable.getURI(), hfileDirPath))
          .build();
      assertTrue(mrjob.run());

      final HFileLoader loader = HFileLoader.create(mConf);
      // There is only one reducer, hence one HFile shard:
      final Path hfilePath = new Path(hfileDirPath, "part-r-00000.hfile");
      loader.load(hfilePath, mOutputTable);

      final FijiTableReader reader = mOutputTable.openTableReader();
      final FijiDataRequest kdr = FijiDataRequest.create("info");
      FijiRowScanner scanner = reader.getScanner(kdr);
      BulkImporterTestUtils.validateImportedRows(scanner, true);
      scanner.close();
      reader.close();
    } finally {
      mFS.delete(hfileDirPath, true);
    }
  }
}
