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

package com.moz.fiji.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

/**
 * Tests the ManagedFijiClient.
 */
public class TestManagedFijiClient extends FijiClientTest {
  private static final Set<String> INSTANCE_TABLES =  ImmutableSet.of("t_1", "t_2", "t_3");

  private FijiURI mClusterURI;
  private Set<String> mInstanceNames;
  private ManagedFijiClient mFijiClient;

  /**
   * Install tables to the supplied Fiji instances.
   *
   * @param fijis to have tables installed in.
   */
  private static void installTables(Set<Fiji> fijis) throws IOException {
    TableLayoutDesc layout = FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/sample_table.json");

    for (Fiji fiji : fijis) {
      InstanceBuilder builder = new InstanceBuilder(fiji);
      for (String table : INSTANCE_TABLES) {
        layout.setName(table);
        builder.withTable(layout);
      }
      builder.build();
    }
  }

  @Before
  public void setUp() throws Exception {
    mClusterURI = createTestHBaseURI();

    Set<Fiji> fijis = Sets.newHashSet();
    fijis.add(createTestFiji(mClusterURI));
    fijis.add(createTestFiji(mClusterURI));
    fijis.add(createTestFiji(mClusterURI));
    installTables(fijis);

    ImmutableSet.Builder<String> instances = ImmutableSet.builder();
    for (Fiji fiji : fijis) {
      instances.add(fiji.getURI().getInstance());
    }

    mInstanceNames = instances.build();
    mFijiClient = new ManagedFijiClient(mClusterURI);
    mFijiClient.start();
  }

  @After
  public void tearDown() throws Exception {
    mFijiClient.stop();
  }

  @Test
  public void testHasAllInstances() throws Exception {
    Assert.assertEquals(mInstanceNames, Sets.newHashSet(mFijiClient.getInstances()));
  }

  @Test
  public void testShouldAllowAccessToVisibleInstancesOnly() throws Exception {
    FijiURI clusterURI = createTestHBaseURI();

    Set<Fiji> fijis = Sets.newHashSet();
    // Create two instances with arbitrary names.
    fijis.add(createTestFiji(clusterURI));
    fijis.add(createTestFiji(clusterURI));
    installTables(fijis);

    Set<String> visibleInstances = Sets.newHashSet();
    ImmutableSet.Builder<String> instances = ImmutableSet.builder();
    for (Fiji fiji : fijis) {
      instances.add(fiji.getURI().getInstance());
    }
    final ImmutableSet<String> instanceNames = instances.build();
    // Select only the first instance to be visible.
    visibleInstances.add(instanceNames.iterator().next());

    ManagedFijiClient fijiClient = new ManagedFijiClient(
        clusterURI,
        ManagedFijiClient.DEFAULT_TIMEOUT,
        visibleInstances);
    fijiClient.start();

    try {
      assertEquals(1, fijiClient.getInstances().size());
    } finally {
      fijiClient.stop();
    }
  }

  @Test
  public void testKeepsFijisCached() throws Exception {
    for (String instance : mInstanceNames) {
      assertTrue(mFijiClient.getFiji(instance) == mFijiClient.getFiji(instance));
    }
  }

  @Test
  public void testKeepsFijiTablesCached() throws Exception {
    for (String instance : mInstanceNames) {
      for (String table : INSTANCE_TABLES) {
        assertTrue(mFijiClient.getFijiTable(instance, table)
            == mFijiClient.getFijiTable(instance, table));
      }
    }
  }

  @Test
  public void testKeepsReadersCached() throws Exception {
    for (String instance : mInstanceNames) {
      for (String table : INSTANCE_TABLES) {
        assertTrue(mFijiClient.getFijiTableReader(instance, table)
            == mFijiClient.getFijiTableReader(instance, table));
      }
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetFijiInvalidInstanceForbidden() throws Exception {
    try {
      mFijiClient.getFiji("foo");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetFijiTableInvalidInstanceForbidden() throws Exception {
    try {
      mFijiClient.getFijiTable("foo", "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetFijiTableInvalidTableNotFound() throws Exception {
    try {
      mFijiClient.getFijiTable(mInstanceNames.iterator().next(), "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }


  @Test(expected = WebApplicationException.class)
  public void testGetReaderInvalidInstanceForbidden() throws Exception {
    try {
      mFijiClient.getFijiTableReader("foo", "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetReaderInvalidTableNotFound() throws Exception {
    try {
      mFijiClient.getFijiTableReader(mInstanceNames.iterator().next(), "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test
  public void testShouldRefreshInstances() throws Exception {
    Fiji fiji = createTestFiji(mClusterURI);
    String instance = fiji.getURI().getInstance();

    Thread.sleep(50); // Give the ZK callback a chance to trigger
    assertTrue(mFijiClient.getInstances().contains(instance));

    deleteTestFiji(fiji);
    Thread.sleep(50); // Give the ZK callback a chance to trigger
    assertFalse(mFijiClient.getInstances().contains(instance));
  }
}
