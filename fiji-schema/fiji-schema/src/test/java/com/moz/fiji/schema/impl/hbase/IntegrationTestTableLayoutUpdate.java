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

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;
import com.moz.fiji.schema.zookeeper.TestUsersTracker.QueueingUsersUpdateHandler;
import com.moz.fiji.schema.zookeeper.UsersTracker;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

public class IntegrationTestTableLayoutUpdate extends AbstractFijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestTableLayoutUpdate.class);

  @Test
  public void testUpdateLayout() throws Exception {

    final FijiURI uri = getFijiURI();
    final String tableName = "IntegrationTestTableLayoutUpdate";
    final FijiURI tableURI = FijiURI.newBuilder(uri).withTableName(tableName).build();
    final String layoutId1 = "1";
    final String layoutId2 = "2";
    final HBaseFiji fiji = (HBaseFiji) Fiji.Factory.open(uri);

    try {
      final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
      layoutDesc.setLayoutId(layoutId1);
      layoutDesc.setName(tableName);
      fiji.createTable(layoutDesc);

      final BlockingQueue<Multimap<String, String>> queue = Queues.newArrayBlockingQueue(10);

      final UsersTracker tracker =
          ZooKeeperUtils
              .newTableUsersTracker(fiji.getZKClient(), tableURI)
              .registerUpdateHandler(new QueueingUsersUpdateHandler(queue));
      try {
        tracker.start();
        // Initial user map should be empty:
        assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(2, TimeUnit.SECONDS));

        FijiTable fijiTable = fiji.openTable(tableName);
        try {
          // We opened a table, user map must contain exactly one entry:
          assertEquals(ImmutableSet.of(layoutId1),
              ImmutableSet.copyOf(queue.poll(2, TimeUnit.SECONDS).values()));

          // Push a layout update (a no-op, but with a new layout ID):
          final TableLayoutDesc newLayoutDesc =
              FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
          newLayoutDesc.setReferenceLayout(layoutId1);
          newLayoutDesc.setLayoutId(layoutId2);
          newLayoutDesc.setName(tableName);

          fiji.modifyTableLayout(newLayoutDesc);

          // The new user map should eventually reflect the new layout ID. There may be an
          // intermediate state of no registered users, and then the table user is re-registered
          // with the new layout.
          Collection<String> users = queue.take().values();
          if (users.isEmpty()) {
            users = queue.take().values();
          }
          assertEquals(ImmutableSet.of(layoutId2),
            ImmutableSet.copyOf(users));

        } finally {
          fijiTable.release();
        }

        // Table is now closed, the user map should become empty:
        assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(2, TimeUnit.SECONDS));
      } finally {
        tracker.close();
      }
    } finally {
      fiji.release();
    }
  }
}
