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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.avro.CellSchema;
import com.moz.fiji.schema.avro.SchemaStorage;
import com.moz.fiji.schema.avro.SchemaType;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

/** Tests the StripValueRowFilter. */
public class TestCellByteSizeAsValueFilter extends FijiClientTest {
  /** Tests that the CellByteSizeAsValueFilter replaces the value by its size, in bytes. */
  @Test
  public void testCellByteSizeAsValueFilter() throws Exception {
    final Fiji fiji = new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE))
            .withRow("row")
                .withFamily("family")
                    .withQualifier("column")
                        .withValue("0123456789")
        .build();

    final FijiTable table = fiji.openTable("table");
    try {
      final EntityId eid = table.getEntityId("row");

      final FijiTableLayout layout = table.getLayout();
      final FijiColumnName column = FijiColumnName.create("family", "column");
      final Map<FijiColumnName, CellSpec> overrides =
          ImmutableMap.<FijiColumnName, CellSpec>builder()
          .put(column, layout.getCellSpec(column)
              .setCellSchema(CellSchema.newBuilder()
                  .setType(SchemaType.INLINE)
                  .setStorage(SchemaStorage.FINAL)
                  .setValue("{\"type\": \"fixed\", \"size\": 4, \"name\": \"Int32\"}")
                  .build()))
          .build();

      final FijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
      try {
        final FijiDataRequest dataRequest = FijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withFilter(new CellByteSizeAsValueFilter()).add(column))
            .build();
        final FijiRowData row = reader.get(eid, dataRequest);
        final GenericData.Fixed fixed32 = row.getMostRecentValue("family", "column");
        final int cellSize = Bytes.toInt(fixed32.bytes());

        // Cell size is: length(MD5-hash) + len(string size) + len(string)
        assertEquals(16 + 1 + 10, cellSize);

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
