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

package com.moz.fiji.mapreduce.shellext;

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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.HFileLoader;
import com.moz.fiji.mapreduce.FijiMRTestLayouts;
import com.moz.fiji.mapreduce.TestingResources;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.lib.bulkimport.BulkImporterTestUtils;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.shell.api.Client;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;

/**
 * Tests bulk-importers.
 * <p>Adapted from IntegationTestCSVBulkImporter</p>
 */
public class IntegrationTestImporterDDL extends AbstractFijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestImporterDDL.class);

  private FijiURI mFijiURI = null;
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
    writeTestResource(mBulkImportInputPath, "import-data/ddlshell-csv-import-input.txt");

    mFijiURI = getFijiURI();
    mFiji = Fiji.Factory.open(mFijiURI, mConf);
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

  @Test
  public void testCSVBulkImporterHFile() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    final Client client = Client.newInstance(mFijiURI);
    try {
      client.executeUpdate("MODULE bulkimport;");
      client.executeUpdate("LOAD DATA INFILE '" + mBulkImportInputPath + "' "
          + "INTO TABLE 'test' THROUGH PATH '" + hfileDirPath + "' "
          + "MAP FIELDS (time, first, last, email, phone) AS ("
          + "  first => info:first_name,\n"
          + "  last => info:last_name,\n"
          + "  email => info:email,\n"
          + "  phone => info:phone,\n"
          + "  time => $TIMESTAMP,\n"
          + "  first => $ENTITY\n"
          + ");");
    } finally {
      System.out.println("Client output:");
      System.out.println("==================================================");
      System.out.println(client.getLastOutput());
      System.out.println("==================================================");
      client.close();
    }

    final FijiTableReader reader = mOutputTable.openTableReader();
    final FijiDataRequest kdr = FijiDataRequest.create("info");
    FijiRowScanner scanner = reader.getScanner(kdr);
    BulkImporterTestUtils.validateImportedRows(scanner, true);
    scanner.close();
    reader.close();
  }

  @Test
  public void testCSVBulkImporterDirect() throws Exception {
    final Client client = Client.newInstance(mFijiURI);
    try {
      client.executeUpdate("MODULE bulkimport;");
      client.executeUpdate("LOAD DATA INFILE '" + mBulkImportInputPath + "' "
          + "INTO TABLE 'test' DIRECT "
          + "MAP FIELDS (time, first, last, email, phone) AS ("
          + "  first => info:first_name,\n"
          + "  last => info:last_name,\n"
          + "  email => info:email,\n"
          + "  phone => info:phone,\n"
          + "  time => $TIMESTAMP,\n"
          + "  first => $ENTITY\n"
          + ");");
    } finally {
      System.out.println("Client output:");
      System.out.println("==================================================");
      System.out.println(client.getLastOutput());
      System.out.println("==================================================");
      client.close();
    }

    final FijiTableReader reader = mOutputTable.openTableReader();
    final FijiDataRequest kdr = FijiDataRequest.create("info");
    FijiRowScanner scanner = reader.getScanner(kdr);
    BulkImporterTestUtils.validateImportedRows(scanner, true);
    scanner.close();
    reader.close();
  }
}
