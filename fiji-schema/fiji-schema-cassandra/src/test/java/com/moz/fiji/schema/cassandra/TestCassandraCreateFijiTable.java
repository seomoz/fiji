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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.VersionInfo;

/** Basic test for creating a simple Fiji table in Cassandra. */
public class TestCassandraCreateFijiTable extends CassandraFijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraCreateFijiTable.class);

  // TODO: Test locality group with max versions = 1 stores only one version.
  // Test creating a table with a locality group where max versions = 1, write multiple
  // versions of the same cell to the table, and then read back from the table with a data request
  // with max versions = infiinity and verify that we get only one result back.

  @Test
  public void testCreateFijiTable() throws Exception {
    LOG.info("Opening an in-memory fiji instance");
    final Fiji fiji = getFiji();

    LOG.info(String.format("Opened fake Fiji '%s'.", fiji.getURI()));

    final FijiSystemTable systemTable = fiji.getSystemTable();
    assertTrue("Client data version should support installed Fiji instance data version",
        VersionInfo.getClientDataVersion().compareTo(systemTable.getDataVersion()) >= 0);

    assertNotNull(fiji.getSchemaTable());
    assertNotNull(fiji.getMetaTable());

    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE_FORMATTED_EID));

    final FijiTable table = fiji.openTable("table");
    try {

      final FijiTableWriter writer = table.openTableWriter();
      try {
        writer.put(table.getEntityId("row1"), "family", "column", 0L, "Value at timestamp 0.");
        writer.put(table.getEntityId("row1"), "family", "column", 1L, "Value at timestamp 1.");
      } finally {
        writer.close();
      }

      final FijiTableReader reader = table.openTableReader();
      try {
        final FijiDataRequest dataRequest = FijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withMaxVersions(100).add("family", "column"))
            .build();

        // Try this as a get.
        final FijiRowData rowData = reader.get(table.getEntityId("row1"), dataRequest);
        String s = rowData.getValue("family", "column", 0L).toString();
        assertEquals(s, "Value at timestamp 0.");

        // Try this as a scan.
        int rowCounter = 0;
        final FijiRowScanner rowScanner = reader.getScanner(dataRequest);
        try {
          // Should be just one row, with two versions.
          for (FijiRowData fijiRowData : rowScanner) {
            LOG.info("Read from scanner! " + fijiRowData.toString());
            // Should see two versions for qualifier "column".
            assertEquals(
                "Value at timestamp 0.", fijiRowData.getValue("family", "column", 0L).toString());
            assertEquals(
                "Value at timestamp 1.", fijiRowData.getValue("family", "column", 1L).toString());
            rowCounter++;
          }
        } finally {
          rowScanner.close();
        }
        assertEquals(1, rowCounter);
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
