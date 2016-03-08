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

package com.moz.fiji.schema.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestInstanceBuilder extends FijiClientTest {
  @Test
  public void testBuilder() throws Exception {
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE));

    final Fiji fiji = new InstanceBuilder(getFiji())
        .withTable("table", layout)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column").withValue(1, "foo1")
                                            .withValue(2, "foo2")
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("column").withValue(100, "foo3")
        .build();

    final FijiTable table = fiji.openTable("table");
    final FijiTableReader reader = table.openTableReader();

    // Verify the first row.
    final FijiDataRequest req = FijiDataRequest.create("family", "column");
    final FijiRowData row1 = reader.get(table.getEntityId("row1"), req);
    assertEquals("foo2", row1.getValue("family", "column", 2).toString());

    // Verify the second row.
    final FijiRowData row2 = reader.get(table.getEntityId("row2"), req);
    assertEquals("foo3", row2.getValue("family", "column", 100).toString());

    ResourceUtils.closeOrLog(reader);
    ResourceUtils.releaseOrLog(table);
  }
}
