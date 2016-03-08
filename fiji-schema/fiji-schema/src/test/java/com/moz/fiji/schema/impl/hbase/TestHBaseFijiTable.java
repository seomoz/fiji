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

package com.moz.fiji.schema.impl.hbase;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.zookeeper.TestUsersTracker.QueueingUsersUpdateHandler;
import com.moz.fiji.schema.zookeeper.UsersTracker;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

public class TestHBaseFijiTable extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseFijiTable.class);

  @Test
  public void testReleasingFijiTableWillCloseItsResources() throws Exception {
    final FijiURI uri = getFiji().getURI();
    final String tableName = "IntegrationTestTableLayoutUpdate";
    final FijiURI tableURI = FijiURI.newBuilder(uri).withTableName(tableName).build();
    final String layoutId1 = "1";

    final HBaseFiji fiji = (HBaseFiji) getFiji();
    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
    layoutDesc.setLayoutId(layoutId1);
    layoutDesc.setName(tableName);
    fiji.createTable(layoutDesc);

    final BlockingQueue<Multimap<String, String>> queue = Queues.newSynchronousQueue();

    final UsersTracker tracker =
        ZooKeeperUtils
            .newTableUsersTracker(fiji.getZKClient(), tableURI)
            .registerUpdateHandler(new QueueingUsersUpdateHandler(queue));
    try {
      tracker.start();
      // Initial user map should be empty:
      assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(1, TimeUnit.SECONDS));

      final FijiTable table = fiji.openTable(tableName);
      try {
        // We opened a table, user map must contain exactly one entry:
        final Multimap<String, String> umap = queue.poll(1, TimeUnit.SECONDS);
        assertEquals(ImmutableSet.of(layoutId1), ImmutableSet.copyOf(umap.values()));
      } finally {
        table.release();
      }
      // Table is now closed, the user map should become empty:
      assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(1, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }
}
