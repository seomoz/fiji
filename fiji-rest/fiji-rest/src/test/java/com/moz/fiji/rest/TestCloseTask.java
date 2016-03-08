/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.io.PrintWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.rest.tasks.CloseTask;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/**
 * Tests the Close task.
 */
public class TestCloseTask extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCloseTask.class);
  private static final String TABLE_NAME = "test_close_task_table";

  private ManagedFijiClient mFijiClient;
  private CloseTask mCloseTask;
  private Fiji mFiji;

  @Before
  public void setUp() throws Exception {
    final FijiURI clusterURI = createTestHBaseURI();
    // Create the instance
    mFiji = createTestFiji(clusterURI);
      TableLayoutDesc layout =
          FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/sample_table.json");
      layout.setName(TABLE_NAME);
      mFiji.createTable(layout);

    mFijiClient = new ManagedFijiClient(clusterURI);
    mFijiClient.start();
    mCloseTask = new CloseTask(mFijiClient);
  }

  @After
  public void tearDown() throws Exception {
    mFijiClient.stop();
  }

  @Test
  public void testCloseInstance() throws Exception {
    final String instanceName = mFiji.getURI().getInstance();
    final Fiji fiji = mFijiClient.getFiji(instanceName);

    Assert.assertEquals(ImmutableList.of(TABLE_NAME), fiji.getTableNames());
    Assert.assertEquals(ImmutableSet.of(instanceName), mFijiClient.getInstances());

    mCloseTask.execute(
        ImmutableMultimap.of(CloseTask.INSTANCE_KEY, instanceName),
        new PrintWriter(System.out));

    // Fiji should be closed at this point
    try {
      fiji.getTableNames();
      Assert.fail("A call to a Fiji method succeeded when it should have failed.");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot get table names in Fiji instance"));
    }
  }

  @Test
  public void testCloseTable() throws Exception {
    final String instanceName = mFiji.getURI().getInstance();
    final Fiji fiji = mFijiClient.getFiji(instanceName);
    final FijiTable table = mFijiClient.getFijiTable(instanceName, TABLE_NAME);
    table.retain().release(); // Will throw if closed

    mCloseTask.execute(
        ImmutableMultimap.of(
            CloseTask.INSTANCE_KEY, instanceName, CloseTask.TABLE_KEY, TABLE_NAME),
        new PrintWriter(System.out));

    // Fiji should still be open
    Assert.assertEquals(ImmutableList.of(TABLE_NAME), fiji.getTableNames());

    // table should be closed at this point
    try {
      table.retain().release(); // Will throw
    } catch (IllegalStateException e) {
      return; // good
    }
    Assert.fail();
  }
}
