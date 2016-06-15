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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/** Tests final columns. */
public class TestFinalColumns extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFinalColumns.class);

  @Test
  public void testFinalColumns() throws Exception {
    final Fiji fiji = getFiji();

    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.FINAL_COLUMN);
    fiji.createTable(layoutDesc);

    final FijiTable table = fiji.openTable("table");
    try {
      final FijiTableWriter writer = table.openTableWriter();
      try {
        final EntityId eid = table.getEntityId("row-key");
        writer.put(eid, "family", "int", 314159);
        writer.put(eid, "family", "long", 314159L);
        writer.put(eid, "family", "double", 3.14159d);

        try {
          writer.put(eid, "family", "long", 123456);
          fail("java.lang.Integer is not an acceptable value for Avro long.");
        } catch (FijiEncodingException kee) {
          assertTrue(kee.getMessage(),
              kee.getMessage().contains("java.lang.Integer cannot be cast to java.lang.Long"));
        }

        try {
          writer.put(eid, "family", "double", 314159);
          fail("java.lang.Integer is not an acceptable value for Avro double.");
        } catch (FijiEncodingException kee) {
          // TODO(SCHEMA-549): The ClassCastException sometimes doesn't include any message.
          //     This might be caused by a flaky dependency on Avro.
          // assertTrue(kee.getMessage(),
          //    kee.getMessage().contains("java.lang.Integer cannot be cast to java.lang.Double"));
        }

        final FijiTableReader reader = table.openTableReader();
        try {
          final FijiRowData row = reader.get(eid, FijiDataRequest.create("family"));
          assertEquals(314159, (int) row.getMostRecentValue("family", "int"));

          // Ensures the long value has not been overwritten:
          assertEquals(314159L, (long) row.getMostRecentValue("family", "long"));
        } finally {
          reader.close();
        }
      } finally {
        writer.close();
      }
    } finally {
      table.release();
    }
  }
}
