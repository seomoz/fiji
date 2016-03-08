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

package com.moz.fiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/**
 * Simple example Cassandra integration test.
 *
 * Just creates a table and inserts some data into it.
 */
public class CassandraSimpleIntegrationTest extends AbstractCassandraFijiIntegrationTest {
  @Test
  public void testSimpleTable() throws Exception {
    // Create the table.

    // Insert some data.

    // Read it back.
    Fiji fiji = getFiji();
    assertNotNull(fiji);
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE_FORMATTED_EID));
    FijiTable mTable = fiji.openTable("table");
    try {
      assertNotNull(mTable);
      // Fill local variables.
      FijiTableReader mReader = mTable.openTableReader();
      try {
        FijiTableWriter mWriter = mTable.openTableWriter();
        try {
          EntityId mEntityId = mTable.getEntityId("eid-foo");

          mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
          mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

          final FijiDataRequest dataRequest = FijiDataRequest.builder()
              .addColumns(
                  FijiDataRequestBuilder.ColumnsDef.create()
                      .withMaxVersions(2)
                      .add("family", "column"))
              .build();

          // Try this as a get.
          FijiRowData rowData = mReader.get(mEntityId, dataRequest);
          String s = rowData.getValue("family", "column", 0L).toString();
          assertEquals(s, "Value at timestamp 0.");

          // Delete the cell and make sure that this value is missing.
          mWriter.deleteCell(mEntityId, "family", "column", 0L);

          rowData = mReader.get(mEntityId, dataRequest);
          assertFalse(rowData.containsCell("family", "column", 0L));
        } finally {
          mWriter.close();
        }
      } finally {
        mReader.close();
      }
    } finally {
      mTable.release();
    }
  }
}
