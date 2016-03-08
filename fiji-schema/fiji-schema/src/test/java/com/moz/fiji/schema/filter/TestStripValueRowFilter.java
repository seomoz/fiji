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

package com.moz.fiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.NavigableSet;

import org.junit.Test;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/** Tests the StripValueRowFilter. */
public class TestStripValueRowFilter extends FijiClientTest {
  /** Verifies that values has been stripped if the StripValueRowFilter has been applied. */
  @Test
  public void testStripValueRowFilter() throws Exception {
    final Fiji fiji = getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE));

    final FijiTable table = fiji.openTable("table");
    try {
      final EntityId eid = table.getEntityId("eid");

      {
        final FijiTableWriter writer = table.openTableWriter();
        try {
          writer.put(eid, "family", "column", 1L, "me");
          writer.put(eid, "family", "column", 2L, "me-too");
        } finally {
          writer.close();
        }
      }

      final FijiTableReader reader = table.openTableReader();
      try {
        final FijiDataRequest dataRequest = FijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "column"))
            .build();
        final FijiRowFilter rowFilter = new StripValueRowFilter();
        final FijiScannerOptions scannerOptions =
            new FijiScannerOptions().setFijiRowFilter(rowFilter);

        final FijiRowScanner scanner = reader.getScanner(dataRequest, scannerOptions);
        try {
          for (FijiRowData row : scanner) {
            final NavigableSet<String> qualifiers = row.getQualifiers("family");
            assertEquals(1, qualifiers.size());
            assertTrue(qualifiers.contains("column"));

            // Ensure that we can use getTimestamps() to count.
            assertEquals(2, row.getTimestamps("family", "column").size());
            try {
              // Cell value is stripped, hence IOException on the wrong schema hash:
              row.getMostRecentValue("family", "column");
              fail("row.getMostRecentValue() did not throw IOException.");
            } catch (IOException ioe) {
              assertTrue(ioe.getMessage(),
                  ioe.getMessage().contains(
                      "Schema with hash 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00 "
                      + "not found in schema table."));
            }
          }
        } finally {
          scanner.close();
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testEqualsAndHashCode() {
    final StripValueRowFilter filter1 = new StripValueRowFilter();
    final StripValueRowFilter filter2 = new StripValueRowFilter();
    assertEquals(filter1, filter2);
    assertEquals(filter1.hashCode(), filter2.hashCode());
  }
}
