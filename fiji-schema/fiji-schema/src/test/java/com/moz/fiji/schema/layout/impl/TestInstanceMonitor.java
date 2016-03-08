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

package com.moz.fiji.schema.layout.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.zookeeper.TestUsersTracker.QueueingUsersUpdateHandler;
import com.moz.fiji.schema.zookeeper.UsersTracker;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

public class TestInstanceMonitor extends FijiClientTest {

  private volatile FijiURI mTableURI;
  private volatile CuratorFramework mZKClient;
  private volatile InstanceMonitor mInstanceMonitor;

  @Before
  public void setUpTestInstanceMonitor() throws Exception {
    Fiji fiji = getFiji();
    TableLayoutDesc layout = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    fiji.createTable(layout);
    mTableURI = FijiURI.newBuilder(fiji.getURI()).withTableName(layout.getName()).build();
    mZKClient = ZooKeeperUtils.getZooKeeperClient(mTableURI);

    mInstanceMonitor = new InstanceMonitor(
        fiji.getSystemTable().getDataVersion(),
        fiji.getURI(),
        fiji.getSchemaTable(),
        fiji.getMetaTable(),
        mZKClient).start();
  }

  @After
  public void tearDownTestInstanceMonitor() throws Exception {
    mInstanceMonitor.close();
    mZKClient.close();
  }

  @Test
  public void testCanRetrieveTableMonitor() throws Exception {
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());
    Assert.assertEquals("layout-1.0", monitor.getLayout().getDesc().getVersion());
  }

  @Test(expected = IllegalStateException.class)
  public void testClosingInstanceMonitorWillCloseTableLayoutMonitor() throws Exception {
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());
    mInstanceMonitor.close();
    monitor.getLayout();
  }

  @Test
  public void testReleasingTableLayoutMonitorWillCloseIt() throws Exception {
    TableLayoutMonitor monitor1 = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());
    monitor1.close();
    TableLayoutMonitor monitor2 = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());
    Assert.assertTrue(monitor1 != monitor2);
  }

  @Test
  public void testReleasingTableLayoutMonitorWillUpdateZooKeeper() throws Exception {
    final BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();
    final UsersTracker tracker =
        ZooKeeperUtils
            .newTableUsersTracker(mZKClient, mTableURI)
            .registerUpdateHandler(new QueueingUsersUpdateHandler(usersQueue));
    try {
      tracker.start();
      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(1, TimeUnit.SECONDS));

      mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());

      Multimap<String, String> registeredUsers = usersQueue.poll(1, TimeUnit.SECONDS);
      Assert.assertEquals(1, registeredUsers.size());
      Assert.assertTrue(registeredUsers.containsValue("1"));

      mInstanceMonitor.close();

      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(1, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }
}
