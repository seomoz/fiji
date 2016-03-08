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
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Test;

import com.moz.fiji.rest.resources.InstanceResource;
import com.moz.fiji.rest.resources.InstancesResource;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiURI;

/**
 * Test class for the Row resource.
 *
 */
public class TestInstancesResources extends ResourceTest {

  private final FijiClientTest mDelegate = new FijiClientTest();
  private ManagedFijiClient mClient;
  private ManagedFijiClient mNullInstanceClient;
  private Set<String> mInstances;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    mDelegate.setupFijiTest();

    FijiURI clusterURI = mDelegate.createTestHBaseURI();

    Fiji fiji1 = mDelegate.createTestFiji(clusterURI);
    Fiji fiji2 = mDelegate.createTestFiji(clusterURI);

    mInstances = Sets.newHashSet(fiji1.getURI().getInstance(), fiji2.getURI().getInstance());

    FijiRESTService.registerSerializers(getObjectMapper());
    mClient = new ManagedFijiClient(clusterURI, ManagedFijiClient.DEFAULT_TIMEOUT, mInstances);
    mClient.start();

    mNullInstanceClient = new ManagedFijiClient(clusterURI,
        ManagedFijiClient.DEFAULT_TIMEOUT,
        null);
    mNullInstanceClient.start();

    addResource(new InstanceResource(mNullInstanceClient));
    addResource(new InstancesResource(mClient));
  }

  /**
   * Runs after each test.
   *
   * @throws Exception
   */
  @After
  public void afterTest() throws Exception {
    mClient.stop();
    mNullInstanceClient.stop();
    mDelegate.teardownFijiTest();
  }

  @Test
  public void testShouldFetchNoInstancesAsConfigured() throws Exception {

    Set<String> oldInstances = new HashSet<String>(mInstances);

    mInstances.clear();
    mClient.refreshInstances();

    final URI resourceURI = UriBuilder.fromResource(InstancesResource.class).build();
    @SuppressWarnings("unchecked")
    final List<Map<String, String>> instances =
        (List<Map<String, String>>) client().resource(resourceURI).get(List.class);

    assertEquals(0, instances.size());
    mInstances.addAll(oldInstances);
    mClient.refreshInstances();

    // Test the restoration to ensure no side effects for future tests.
    testShouldFetchAllAvailableInstances();
  }

  @Test
  public void testShouldFetchAllAvailableInstances() throws Exception {
    final URI resourceURI = UriBuilder.fromResource(InstancesResource.class).build();
    @SuppressWarnings("unchecked")
    final List<Map<String, String>> instances =
        (List<Map<String, String>>) client().resource(resourceURI).get(List.class);

    final Set<String> instanceNames = Sets.newHashSet();
    for (Map<String, String> instance : instances) {
      instanceNames.add(instance.get("name"));
    }

    assertEquals(mInstances, instanceNames);

    // Ensure that we can fetch something from the instance resource for each instance.
    for (String instance : instanceNames) {
      URI instanceURI = UriBuilder.fromResource(InstanceResource.class).build(instance);
      JsonNode backup = client().resource(instanceURI).get(JsonNode.class);
      assertEquals(backup.get("layout_version").asText(), "system-2.0");
      assertNotNull(backup);
    }
  }
}
