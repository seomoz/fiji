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

package com.moz.fiji.schema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.platform.SchemaPlatformBridge;
import com.moz.fiji.schema.util.TestingFileUtils;

/**
 * Base class for tests that interact with fiji as a client.
 *
 * <p> Provides MetaTable and FijiSchemaTable access. </p>
 * <p>
 *   By default, this base class generate fake HBase instances, for testing.
 *   By setting a JVM system property, this class may be configured to use a real HBase instance.
 *   For example, to use an HBase mini-cluster running on <code>localhost:2181</code>, you may use:
 *   <pre>
 *     mvn clean test \
 *         -Dcom.moz.fiji.schema.FijiClientTest.HBASE_ADDRESS=localhost:2181
 *   </pre>
 *   If you specify the HBASE_ADDRESS property, you may specify both the quorum hosts and the
 *   port to connect on.  If you don't specify the port, it defaults to 2181 (via the semantics
 *   of FijiURI).
 * </p>
 */
public class FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(FijiClientTest.class);

  static {
    SchemaPlatformBridge.get().initializeHadoopResources();
  }

  /**
   * Externally configured address of an HBase cluster to use for testing.
   * Null when unspecified, which means use fake HBase instances.
   */
  private static final String HBASE_ADDRESS =
      System.getProperty("com.moz.fiji.schema.FijiClientTest.HBASE_ADDRESS", null);

  // JUnit requires public, checkstyle disagrees:
  // CSOFF: VisibilityModifierCheck
  /** Test method name (eg. "testFeatureX"). */
  @Rule
  public final TestName mTestName = new TestName();
  // CSON: VisibilityModifierCheck

  /** Counter for fake HBase instances. */
  private final AtomicLong mFakeHBaseInstanceCounter = new AtomicLong();

  /** Counter for test Fiji instances. */
  private final AtomicLong mFijiInstanceCounter = new AtomicLong();

  /** Test identifier, eg. "org_package_ClassName_testMethodName". */
  private String mTestId;

  /** Fiji instances opened during test, and that must be released and cleaned up after. */
  private List<Fiji> mFijis = Lists.newArrayList();

  /** Local temporary directory, automatically cleaned up after. */
  private File mLocalTempDir = null;

  /** Default test Fiji instance. */
  private Fiji mFiji = null;

  /** The configuration object for this fiji instance. */
  private Configuration mConf;

  /**
   * Initializes the in-memory fiji for testing.
   *
   * @throws Exception on error.
   */
  @Before
  public final void setupFijiTest() throws Exception {
    try {
      doSetupFijiTest();
    } catch (Exception exn) {
      // Make exceptions from setup method visible:
      exn.printStackTrace();
      throw exn;
    }
  }

  private void doSetupFijiTest() throws Exception {
    mTestId =
        String.format("%s_%s", getClass().getName().replace('.', '_'), mTestName.getMethodName());
    mLocalTempDir = TestingFileUtils.createTempDir(mTestId, "temp-dir");
    mConf = HBaseConfiguration.create();
    mConf.set("fs.defaultFS", "file://" + mLocalTempDir);
    mConf.set("mapred.job.tracker", "local");
    // If HBASE_ADDRESS was specified, that should be in the conf for all methods of this test.
    // Otherwise don't specify hbase.zookeeper.quorum; it will be a different fake-hbase instance
    // for each method.
    if (null != HBASE_ADDRESS) {
      FijiURI zkURI = FijiURI.newBuilder("fiji://" + HBASE_ADDRESS).build();
      String quorum = zkURI.getZooKeeperEnsemble();
      String port = Integer.toString(zkURI.getZookeeperClientPort());
      mConf.set("hbase.zookeeper.quorum", quorum);
      mConf.set("hbase.zookeeper.property.clientPort", port);
    }
    mFiji = null;  // lazily initialized
  }

  /**
   * Creates a test HBase URI.
   *
   * <p>
   *   This HBase instance is ideally made unique for each test, but there is no hard guarantee.
   *   In particular, the HBase instance is shared with other tests when running against an
   *   external HBase cluster.
   *   Thus, you must clean after yourself by removing tables you create in your tests.
   * </p>
   *
   * @return the FijiURI of a test HBase instance.
   */
  public FijiURI createTestHBaseURI() {
    final long fakeHBaseCounter = mFakeHBaseInstanceCounter.getAndIncrement();
    final String testName =
        String.format("%s_%s", getClass().getSimpleName(), mTestName.getMethodName());
    final String hbaseAddress =
        (HBASE_ADDRESS != null)
        ? HBASE_ADDRESS
        : String.format(".fake.%s-%d", testName, fakeHBaseCounter);
    return FijiURI.newBuilder(String.format("fiji://%s", hbaseAddress)).build();
  }

  /**
   * Creates and opens a new unique test Fiji instance in a new fake HBase cluster.  All generated
   * Fiji instances are automatically cleaned up by FijiClientTest.
   *
   * @return a fresh new Fiji instance in a new fake HBase cluster.
   * @throws Exception on error.
   */
  public Fiji createTestFiji() throws Exception {
    return createTestFiji(createTestHBaseURI());
  }

  /**
   * Creates and opens a new unique test Fiji instance in the specified cluster.  All generated
   * Fiji instances are automatically cleaned up by FijiClientTest.
   *
   * @param clusterURI of cluster create new instance in.
   * @return a fresh new Fiji instance in the specified cluster.
   * @throws Exception on error.
   */
  public Fiji createTestFiji(FijiURI clusterURI) throws Exception {
    Preconditions.checkNotNull(mConf);
    final String instanceName = String.format("%s_%s_%d",
        getClass().getSimpleName(),
        mTestName.getMethodName(),
        mFijiInstanceCounter.getAndIncrement());
    final FijiURI uri = FijiURI.newBuilder(clusterURI).withInstanceName(instanceName).build();
    FijiInstaller.get().install(uri, mConf);
    final Fiji fiji = Fiji.Factory.open(uri, mConf);

    mFijis.add(fiji);
    return fiji;
  }

  /**
   * Deletes a test Fiji instance. The <code>Fiji</code> reference provided to this method will no
   * longer be valid after it returns (it will be closed).  Calling this method on Fiji instances
   * created through FijiClientTest is not necessary, it is provided for testing situations in which
   * a Fiji is explicitly closed.
   *
   * @param fiji instance to be closed and deleted.
   * @throws Exception on error.
   */
  public void deleteTestFiji(Fiji fiji) throws Exception {
    Preconditions.checkState(mFijis.contains(fiji));
    fiji.release();
    FijiInstaller.get().uninstall(fiji.getURI(), mConf);
    mFijis.remove(fiji);
  }

  /**
   * Closes the in-memory fiji instance.
   * @throws Exception If there is an error.
   */
  @After
  public final void teardownFijiTest() throws Exception {
    LOG.debug("Tearing down {}", mTestId);
    for (Fiji fiji : mFijis) {
      fiji.release();
      FijiInstaller.get().uninstall(fiji.getURI(), mConf);
    }
    mFijis = null;
    mFiji = null;
    mConf = null;
    FileUtils.deleteDirectory(mLocalTempDir);
    mLocalTempDir = null;
    mTestId = null;
  }

  /**
   * Gets the default Fiji instance to use for testing.
   *
   * @return the default Fiji instance to use for testing.
   *     Automatically released by FijiClientTest.
   * @throws IOException on I/O error.  Should be Exception, but breaks too many tests for now.
   */
  public synchronized Fiji getFiji() throws IOException {
    if (null == mFiji) {
      try {
        mFiji = createTestFiji();
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception exn) {
        // TODO: Remove wrapping:
        throw new IOException(exn);
      }
    }
    return mFiji;
  }

  /** @return a valid identifier for the current test. */
  public String getTestId() {
    return mTestId;
  }

  /** @return a local temporary directory. */
  public File getLocalTempDir() {
    return mLocalTempDir;
  }

  /**
   * @return a test Hadoop configuration, with:
   *     <li> a default FS
   *     <li> a job tracker
   */
  public Configuration getConf() {
    return mConf;
  }
}
