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

package com.moz.fiji.schema.cassandra;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiInstaller;
import com.moz.fiji.schema.testutil.IntegrationHelper;
import com.moz.fiji.schema.testutil.ToolResult;
import com.moz.fiji.schema.tools.BaseTool;
import com.moz.fiji.schema.util.Debug;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * A base class for all Cassandra Fiji integration tests.
 *
 * This class sets up a Fiji instance before each test and tears it down afterwards.
 * It assumes there is a Cassandra cluster running already and that there is a `hbase-site.xml`
 * file. Starting a bento cluster places this file in the target/test-classes directory.
 *
 * To avoid stepping on other Fiji instances, the name of the instance created is
 * a random unique identifier.
 *
 * This class is abstract because it has a lot of boilerplate for setting up integration
 * tests but doesn't actually test anything.
 *
 * The STANDALONE variable controls whether the test creates an embedded HBase and M/R mini-cluster
 * for itself. This allows a single test to run in a debugger without external setup.
 */
public abstract class AbstractCassandraFijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCassandraFijiIntegrationTest.class);

  /**
   * Name of the property to specify an external HBase instance.
   *
   * From Maven, one may specify an external cluster with:
   *   mvn clean verify \
   *   -Dfiji.test.cassandra.cluster.uri=fiji-cassandra://localhost:2181/cassandrahost/9140
   */
  private static final String BASE_TEST_URI_PROPERTY = "fiji.test.cassandra.cluster.uri";

  /** An integration helper for installing and removing instances. */
  private IntegrationHelper mHelper;

  /** Base URI to use for creating all of the instances for different tests. */
  private static FijiURI mBaseUri;

  private static AtomicInteger mFijiCounter = new AtomicInteger();

  // -----------------------------------------------------------------------------------------------

  /** Test configuration. */
  private Configuration mConf;

  /** The randomly generated URI for the instance. */
  private FijiURI mFijiUri;

  private Fiji mFiji;

  // JUnit requires public, checkstyle disagrees:
  // CSOFF: VisibilityModifierCheck
  /** Test method name (eg. "testFeatureX"). */
  @Rule
  public final TestName mTestName = new TestName();

  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /**
   * Creates a Configuration object for the HBase instance to use.
   *
   * @return an HBase configuration to work against.
   */
  protected Configuration createConfiguration() {
    final Configuration conf = HBaseConfiguration.create();

    // Set the mapred.output.dir to a unique temporary directory for each integration test:
    final String tempDir = conf.get("hadoop.tmp.dir");
    assertNotNull(
        "hadoop.tmp.dir must be set in the configuration for integration tests to use.",
        tempDir);
    conf.set("mapred.output.dir", new Path(tempDir, UUID.randomUUID().toString()).toString());

    return conf;
  }

  @BeforeClass
  public static void createBaseUri() {
    final Configuration conf = HBaseConfiguration.create();

    if (System.getProperty(BASE_TEST_URI_PROPERTY) != null) {
      mBaseUri = FijiURI.newBuilder(System.getProperty(BASE_TEST_URI_PROPERTY)).build();
    } else {
      // Create a Fiji instance.
      mBaseUri = FijiURI.newBuilder(String.format(
          "fiji-cassandra://%s:%s/%s:%s/",
          conf.get(HConstants.ZOOKEEPER_QUORUM),
          conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
          conf.get(HConstants.ZOOKEEPER_QUORUM),
          // The default port of the bento cluster's cassandra contact point.
          9042
      )).build();
      LOG.info("Base URI for Cassandra integration tests = ", mBaseUri.toString());
    }
  }

  @Before
  public final void setupFijiIntegrationTest() throws Exception {
    mConf = createConfiguration();

    String instanceName = "it" + mFijiCounter.getAndIncrement();

    mFijiUri = FijiURI.newBuilder(mBaseUri).withInstanceName(instanceName).build();

    LOG.info("Installing to URI " + mFijiUri);

    try {
      CassandraFijiInstaller.get().install(mFijiUri, null);
      LOG.info("Created Fiji instance at " + mFijiUri);
    } catch (IOException ioe) {
      LOG.warn("Could not create Fiji instance.");
      assertTrue("Did not start.", false);
    }

    mFiji = Fiji.Factory.open(mFijiUri);
    Preconditions.checkNotNull(mFiji);

    LOG.info("Setup summary for {}", getClass().getName());
    Debug.logConfiguration(mConf);

    // Get a new Fiji instance, with a randomly-generated name.
    mHelper = new IntegrationHelper(mConf);
  }

  public Fiji getFiji() {
    assertNotNull(mFiji);
    return mFiji;
  }

  @After
  public final void teardownFijiIntegrationTest() throws Exception {
    if (null != mFiji) {
      mFiji.release();
    }
    mFiji = null;
    mHelper = null;
    mFijiUri = null;
    mConf = null;
  }

  /** @return The FijiURI for this test instance. */
  protected FijiURI getFijiURI() {
    return mFijiUri;
  }

  /** @return The name of the instance installed for this test. */
  protected String getInstanceName() {
    return mFijiUri.getInstance();
  }

  /** @return The temporary directory to use for test data. */
  protected File getTempDir() {
    return mTempDir.getRoot();
  }

  /** @return a test Configuration, with a MapReduce and HDFS cluster. */
  public Configuration getConf() {
    return mConf;
  }

  /**
   * Gets the path to a file in the mini HDFS cluster.
   *
   * <p>If the file does not yet exist, it will be copied into the cluster first.</p>
   *
   * @param localResource A local file resource.
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  protected Path getDfsPath(URL localResource) throws IOException {
    return getDfsPath(localResource.getPath());
  }


  /**
   * Gets the path to a file in the mini HDFS cluster.
   *
   * <p>If the file does not yet exist, it will be copied into the cluster first.</p>
   *
   * @param localPath A local file path (doesn't have to exist, but if it does it will be copied).
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  protected Path getDfsPath(String localPath) throws IOException {
    return getDfsPath(new File(localPath));
  }

  /**
   * Gets the path to a file in the mini HDFS cluster.
   *
   * <p>If the file does not yet exist, it will be copied into the cluster first.</p>
   *
   * @param localFile A local file.
   * @return The path to the file in the mini HDFS filesystem.
   * @throws java.io.IOException If there is an error.
   */
  protected Path getDfsPath(File localFile) throws IOException {
    String uniquePath
        = new File(getClass().getName().replaceAll("\\.\\$", "/"), localFile.getPath()).getPath();
    if (localFile.exists()) {
      return mHelper.copyToDfs(localFile, uniquePath);
    }
    return mHelper.getDfsPath(uniquePath);
  }

  /**
   * Runs a tool within the instance for this test and captures the console output.
   *
   * @param tool The tool to run.
   * @param args The command-line args to pass to the tool.  The --instance flag will be added.
   * @return A result with the captured tool output.
   * @throws Exception If there is an error.
   */
  protected ToolResult runTool(BaseTool tool, String[] args) throws Exception {
    // Append the --instance=<instance-name> flag on the end of the args.
    final String[] argsWithFiji = Arrays.copyOf(args, args.length + 1);
    argsWithFiji[args.length] = "--debug=true";
    return mHelper.runTool(mHelper.getConf(), tool, argsWithFiji);
  }

  /**
   * Creates and populates a test table of users called 'foo'.
   *
   * @throws Exception If there is an error.
   */
  protected void createAndPopulateFooTable() throws Exception {
    mHelper.createAndPopulateFooTable(mFijiUri);
  }

  /**
   * Deletes the table created with createAndPopulateFooTable().
   *
   * @throws Exception If there is an error.
   */
  public void deleteFooTable() throws Exception {
    mHelper.deleteFooTable(mFijiUri);
  }

  /**
   * Formats an exception stack trace into a string.
   *
   * @param exn Exception to format.
   * @return the exception stack trace, as a string.
   */
  protected static String formatException(Exception exn) {
    final StringWriter writer = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(writer);
    exn.printStackTrace(printWriter);
    ResourceUtils.closeOrLog(printWriter);
    return writer.toString();
  }
}
