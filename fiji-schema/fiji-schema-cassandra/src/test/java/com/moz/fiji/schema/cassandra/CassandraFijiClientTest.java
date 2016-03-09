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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.checkin.CheckinUtils;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.cassandra.util.SessionCache;
import com.moz.fiji.schema.impl.cassandra.CassandraFiji;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiFactory;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiInstaller;

/**
 * Base class for Cassandra tests that interact with fiji as a client.
 *
 * <p> Provides MetaTable and FijiSchemaTable access. </p>
 */
public class CassandraFijiClientTest {
  /*
   * <p>
   *   // TODO: Implement ability to connect to a real Cassandra service in tests.
   *   By default, this base class connects to an EmbeddedCassandraService. By setting a JVM
   *   system property, this class may be configured to use a real Cassandra instance. For example,
   *   to use an C* node running on <code>localhost:2181</code>, you may use:
   *   <pre>
   *     mvn clean test \
   *         -DargLine="-Dcom.moz.fiji.schema.CassandraFijiClientTest.CASSANDRA_ADDRESS=localhost:2181"
   *   </pre>
   * </p>
   */
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiClientTest.class);

  //static { SchemaPlatformBridge.get().initializeHadoopResources(); }

  /**
   * Externally configured address of a C* cluster to use for testing.
   * Null when unspecified, which means use EmbeddedCassandraService.
   */
  private static final String CASSANDRA_ADDRESS =
      System.getProperty("com.moz.fiji.schema.CassandraFijiClientTest.CASSANDRA_ADDRESS", null);

  // JUnit requires public, checkstyle disagrees:
  // CSOFF: VisibilityModifierCheck
  /** Test method name (eg. "testFeatureX"). */
  @Rule
  public final TestName mTestName = new TestName();
  // CSON: VisibilityModifierCheck

  /** Dummy session to hold open a cached connection to Cassandra. */
  private static Session mSession = null;

  /**
   * This is a hack to open a cached session before each test class, so that the session can be
   * opened once to avoid cache-thrashing.
   */
  @BeforeClass
  public static void setupClass() throws IOException {
    mSession =
        SessionCache.getSession(
            EmbeddedCassandra.getContactPoints(),
            EmbeddedCassandra.getContactPort(),
            null,
            null);
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    mSession.closeAsync();
  }

  /** Counter for fake C* instances. */
  private static final AtomicLong FAKE_CASSANDRA_INSTANCE_COUNTER = new AtomicLong();

  /** Counter for test Fiji instances. */
  private static final AtomicLong FIJI_INSTANCE_COUNTER = new AtomicLong();

  /** Test identifier, eg. "org_package_ClassName_testMethodName". */
  private String mTestId;

  /** Fiji instances opened during test, and that must be released and cleaned up after. */
  private List<Fiji> mAllFijis = Lists.newArrayList();

  /** Default test Fiji instance. */
  private CassandraFiji mFiji = null;

  /**
   * Initializes the in-memory fiji for testing.
   *
   * @throws IOException on error.
   */
  @Before
  public final void setupFijiTest() throws IOException {
    try {
      doSetupFijiTest();
    } catch (Exception exn) {
      // Make exceptions from setup method visible:
      exn.printStackTrace();
      throw exn;
    }
  }

  private void doSetupFijiTest() throws IOException {
    LOG.info("Setting up Cassandra Fiji client tests...");
    mTestId =
        String.format("%s_%s", getClass().getName().replace('.', '_'), mTestName.getMethodName());
    mFiji = null;  // lazily initialized
    // Disable logging of commands to the upgrade server by accident.
    System.setProperty(CheckinUtils.DISABLE_CHECKIN_PROP, "true");
  }

  /**
   * Creates a test C* URI.
   *
   * @return the FijiURI of a test HBase instance.
   */
  public CassandraFijiURI createTestCassandraURI() throws IOException {
    final long fakeCassandraCounter = FAKE_CASSANDRA_INSTANCE_COUNTER.getAndIncrement();
    final String testName = String.format(
        "%s_%s",
        getClass().getSimpleName(),
        mTestName.getMethodName());

    if (CASSANDRA_ADDRESS != null) {
      return CassandraFijiURI.newBuilder(CASSANDRA_ADDRESS).build();
    }

    // Goes into the ZooKeeper section of the URI.
    final String zookeeperQuorum = String.format(".fake.%s-%d", testName, fakeCassandraCounter);

    CassandraFijiURI uri = CassandraFijiURI
        .newBuilder()
        .withZookeeperQuorum(ImmutableList.of(zookeeperQuorum))
        .withContactPoints(EmbeddedCassandra.getContactPoints())
        .withContactPort(EmbeddedCassandra.getContactPort())
        .build();

    LOG.debug("Created test Cassandra URI: '{}'.", uri);
    return uri;
  }

  /**
   * Opens a new unique test Fiji instance, creating it if necessary.
   *
   * Each call to this method returns a fresh new Fiji instance.
   * All generated Fiji instances are automatically cleaned up by CassandraFijiClientTest.
   *
   * @return a fresh new Fiji instance.
   * @throws IOException on error.
   */
  public CassandraFiji createTestFiji() throws IOException {
    // Note: The C* keyspace for the instance has to be less than 48 characters long. Every C*
    // Fiji keyspace starts with "fiji_", so we have a total of 43 characters to work with - yikes!
    // Hopefully dropping off the class name is good enough to make this short enough.

    final String instanceName =
        String.format("%s_%d", mTestName.getMethodName(), FIJI_INSTANCE_COUNTER.getAndIncrement());

    LOG.info("Creating a test Fiji instance.  Calling Fiji instance " + instanceName);

    final CassandraFijiURI fijiURI = createTestCassandraURI();
    final CassandraFijiURI instanceURI =
        CassandraFijiURI.newBuilder(fijiURI).withInstanceName(instanceName).build();
    LOG.info("Installing fake C* instance " + instanceURI);
    CassandraFijiInstaller.get().install(instanceURI, null);
    final CassandraFiji fiji = CassandraFijiFactory.get().open(instanceURI);

    mAllFijis.add(fiji);
    return fiji;
  }

  /**
   * Closes the in-memory fiji instance.
   * @throws IOException If there is an error.
   */
  @After
  public final void tearDownFijiTest() throws IOException {
    LOG.debug("Tearing down {}", mTestId);
    for (Fiji fiji : mAllFijis) {
      fiji.release();
      CassandraFijiInstaller.get().uninstall(fiji.getURI(), null);
    }
    mAllFijis = null;
    mFiji = null;
    mTestId = null;

    // Force a garbage collection, to trigger finalization of resources and spot
    // resources that were not released or closed.
    System.gc();
    System.runFinalization();
  }

  /**
   * Gets the default Fiji instance to use for testing.
   *
   * @return the default Fiji instance to use for testing.
   *     Automatically released by FijiClientTest.
   * @throws IOException on I/O error.
   */
  public synchronized CassandraFiji getFiji() throws IOException {
    if (null == mFiji) {
      mFiji = createTestFiji();
    }
    return mFiji;
  }

  /**
   * Static, global, embedded Cassandra instance. Public for testing.
   */
  public static final class EmbeddedCassandra {

    @GuardedBy("this")
    private static EmbeddedCassandraService embeddedCassandraService;

    public static List<String> getContactPoints() throws IOException {
      ensureEmbeddedCassandra();
      return ImmutableList.of(DatabaseDescriptor.getListenAddress().getHostName());
    }

    public static int getContactPort() throws IOException {
      ensureEmbeddedCassandra();
      return DatabaseDescriptor.getNativeTransportPort();
    }

    /**
     * Start an embedded Cassandra service for testing, if it is not already started.
     */
    private static synchronized void ensureEmbeddedCassandra() throws IOException {
      if (embeddedCassandraService != null) {
        return;
      }

      LOG.info("Starting embedded Cassandra service.");

      // Use a custom YAML file that specifies different ports from normal for RPC and thrift.
      InputStream yamlStream = EmbeddedCassandra.class.getResourceAsStream("/cassandra.yaml");
      Preconditions.checkState(yamlStream != null, "Unable to find resource '/cassandra.yaml'.");

      // Update cassandra.yaml to use available ports.
      String cassandraYaml = IOUtils.toString(yamlStream);

      final int storagePort = findOpenPort(); // Normally 7000.
      final int nativeTransportPort = findOpenPort(); // Normally 9042.

      cassandraYaml = updateCassandraYaml(cassandraYaml, "__STORAGE_PORT__", storagePort);
      cassandraYaml =
          updateCassandraYaml(cassandraYaml, "__NATIVE_TRANSPORT_PORT__", nativeTransportPort);

      // Write out the YAML contents to a temp file.
      File yamlFile = File.createTempFile("cassandra", ".yaml");
      LOG.info("Writing cassandra.yaml to {}", yamlFile);
      try (FileWriter fw = new FileWriter(yamlFile);
           BufferedWriter bw = new BufferedWriter(fw)) {
        bw.write(cassandraYaml);
      }

      Preconditions.checkArgument(yamlFile.exists());
      System.setProperty("cassandra.config", "file:" + yamlFile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");

      // Make sure that all of the directories for the commit log, data, and caches are empty.
      // Thank goodness there are methods to get this information (versus parsing the YAML
      // directly).
      List<String> directoriesToDelete =
          Lists.newArrayList(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()));
      directoriesToDelete.add(DatabaseDescriptor.getCommitLogLocation());
      directoriesToDelete.add(DatabaseDescriptor.getSavedCachesLocation());
      for (String directory : directoriesToDelete) {
        FileUtils.deleteDirectory(new File(directory));
      }

      embeddedCassandraService = new EmbeddedCassandraService();
      embeddedCassandraService.start();
    }

    /**
     * Update a stringified cassandra.yaml containing a label with with a given port.
     *
     * @param yaml The stringified cassandra.yaml containing a label to substitue.
     * @param label The label to substitute for the port.
     * @param port The port to substitute for the label.
     * @return The contents of the YAML file after the substitution.
     */
    private static String updateCassandraYaml(
        String yaml,
        String label,
        int port
    ) {
      String yamlContentsAfterSub = yaml.replace(label, Integer.toString(port));
      Preconditions.checkArgument(!yamlContentsAfterSub.equals(yaml));
      return yamlContentsAfterSub;
    }

    /**
     * Find an available port.
     *
     * @return An open port number.
     * @throws IOException on I/O error.
     */
    private static int findOpenPort() throws IOException {
      ServerSocket serverSocket = new ServerSocket(0);
      int portNumber = serverSocket.getLocalPort();
      serverSocket.setReuseAddress(true);
      serverSocket.close();
      return portNumber;
    }
  }
}
