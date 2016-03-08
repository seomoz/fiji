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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Queues;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.zookeeper.TableLayoutTracker;
import com.moz.fiji.schema.zookeeper.TestTableLayoutTracker.QueuingTableLayoutUpdateHandler;

public class IntegrationTestHBaseTableLayoutUpdater extends AbstractFijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestHBaseTableLayoutUpdater.class);

  private static final String LAYOUT_V1 = "com.moz.fiji/schema/layout/layout-updater-v1.json";
  private static final String LAYOUT_V2 = "com.moz.fiji/schema/layout/layout-updater-v2.json";

  /**
   * Basic test for the flow of a table layout update:
   * Create a new table, then update its layout,
   * while simulating a single live client using the table.
   */
  @Test
  public void testCreateTable() throws Exception {
    final TableLayoutDesc layout1 = FijiTableLayouts.getLayout(LAYOUT_V1);
    final TableLayoutDesc layout2 = FijiTableLayouts.getLayout(LAYOUT_V2);

    final FijiURI uri = getFijiURI();

    // Update the data version of the Fiji instance:
    {
      final Fiji fiji = Fiji.Factory.open(uri);
      try {
        fiji.getSystemTable().setDataVersion(ProtocolVersion.parse("system-2.0"));
      } finally {
        fiji.release();
      }
    }

    final Fiji fiji = Fiji.Factory.open(uri);
    try {
      fiji.createTable(layout1);

      final FijiTable table = fiji.openTable("table_name");
      try {
        final BlockingQueue<String> layoutQueue = Queues.newArrayBlockingQueue(1);

        final TableLayoutTracker tracker =
            new TableLayoutTracker(((HBaseFiji) fiji).getZKClient(), table.getURI(),
                new QueuingTableLayoutUpdateHandler(layoutQueue))
              .start();

        Assert.assertEquals("1", layoutQueue.poll(5, TimeUnit.SECONDS));

        final HBaseTableLayoutUpdater updater =
            new HBaseTableLayoutUpdater((HBaseFiji) fiji, table.getURI(), layout2);
        try {
          final Thread thread =
              new Thread() {
                /** {@inheritDoc} */
                @Override
                public void run() {
                  try {
                    updater.update();
                  } catch (Exception exn) {
                    throw new RuntimeException(exn);
                  }
                }
              };
          thread.start();
          thread.join(5000);

          Assert.assertEquals("2", layoutQueue.poll(5, TimeUnit.SECONDS));

          tracker.close();
        } finally {
          updater.close();
        }
      } finally {
        table.release();
      }
      fiji.deleteTable("table_name");
    } finally {
      fiji.release();
    }
  }
}
