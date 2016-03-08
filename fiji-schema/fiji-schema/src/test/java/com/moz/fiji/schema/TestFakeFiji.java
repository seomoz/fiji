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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.VersionInfo;

/** Basic tests for a fake Fiji instance. */
public class TestFakeFiji extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFakeFiji.class);

  @Test
  public void testFakeFiji() throws Exception {
    LOG.info("Opening an in-memory fiji instance");
    final FijiURI uri = FijiURI
        .newBuilder(String.format("fiji://.fake.%s/instance", getTestId()))
        .build();
    final Configuration conf = HBaseConfiguration.create();
    FijiInstaller.get().install(uri, conf);

    final Fiji fiji = Fiji.Factory.open(uri, conf);
    try {
      LOG.info(String.format("Opened fake Fiji '%s'.", fiji.getURI()));

      final FijiSystemTable systemTable = fiji.getSystemTable();
      assertTrue("Client data version should support installed Fiji instance data version",
          VersionInfo.getClientDataVersion().compareTo(systemTable.getDataVersion()) >= 0);

      assertNotNull(fiji.getSchemaTable());
      assertNotNull(fiji.getMetaTable());

      fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE));
      final FijiTable table = fiji.openTable("table");
      try {
        {
          final FijiTableReader reader = table.openTableReader();
          try {
            final FijiDataRequest dataRequest = FijiDataRequest.builder()
                .addColumns(ColumnsDef.create().addFamily("family"))
                .build();
            final FijiRowScanner scanner = reader.getScanner(dataRequest);
            try {
              assertFalse(scanner.iterator().hasNext());
            } finally {
              scanner.close();
            }
          } finally {
            reader.close();
          }
        }

        {
          final FijiTableWriter writer = table.openTableWriter();
          try {
            writer.put(table.getEntityId("row1"), "family", "column", "the string value");
          } finally {
            writer.close();
          }
        }

        {
          final FijiTableReader reader = table.openTableReader();
          try {
            final FijiDataRequest dataRequest = FijiDataRequest.builder()
                .addColumns(ColumnsDef.create().addFamily("family"))
                .build();
            final FijiRowScanner scanner = reader.getScanner(dataRequest);
            try {
              final Iterator<FijiRowData> it = scanner.iterator();
              assertTrue(it.hasNext());
              FijiRowData row = it.next();
              assertEquals("the string value",
                  row.getMostRecentValue("family", "column").toString());
              assertFalse(it.hasNext());
            } finally {
              scanner.close();
            }
          } finally {
            reader.close();
          }
        }
      } finally {
        table.release();
      }
    } finally {
      fiji.release();
    }
  }
}
