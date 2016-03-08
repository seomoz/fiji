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

package com.moz.fiji.schema.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.RowKeyEncoding;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestScanTool extends FijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestScanTool.class);

  @Test
  public void testUnderspecified() throws Exception {
    final Fiji fiji = getFiji();
    final FijiURI hbaseURI = FijiURI.newBuilder(fiji.getURI()).withInstanceName(null).build();

    assertEquals(BaseTool.FAILURE, runTool(new ScanTool(), hbaseURI.toString()));
    assertTrue(mToolOutputLines[0].startsWith("Specify a cluster"));
    assertEquals(BaseTool.FAILURE, runTool(new ScanTool()));
    assertTrue(mToolOutputLines[0].startsWith("URI must be specified"));
    assertEquals(BaseTool.FAILURE, runTool(new ScanTool(), hbaseURI.toString(), "--max-rows=-1"));
    assertTrue(mToolOutputLines[0].startsWith("--max-rows must be nonnegative"));
    assertEquals(BaseTool.FAILURE, runTool(new ScanTool(),
        hbaseURI.toString(),
        "--max-versions=0"));
    assertTrue(mToolOutputLines[0].startsWith("--max-versions must be positive"));
    assertEquals(BaseTool.FAILURE, runTool(new ScanTool(),
        hbaseURI.toString() + "instance/table",
        "--timestamp="));
    assertTrue(mToolOutputLines[0].startsWith("--timestamp"));
  }

  @Test
  public void testScanTable() throws Exception {
    final Fiji fiji = getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE));
    final FijiTable table = fiji.openTable("table");
    try {
      // Table is empty:
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(), table.getURI().toString()));
      assertEquals(1, mToolOutputLines.length);
      assertTrue(mToolOutputLines[0].startsWith("Scanning fiji table: "));

      new InstanceBuilder(fiji)
          .withTable(table)
              .withRow("hashed")
                  .withFamily("family").withQualifier("column").withValue(314L, "value")
          .build();

      // Table has now one row:
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(), table.getURI().toString()));
      assertEquals(3, mToolOutputLines.length);
      assertTrue(mToolOutputLines[0].startsWith("Scanning fiji table: "));
      assertTrue(mToolOutputLines[1].startsWith("entity-id=hbase=hex:"));

    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testFormattedRowKey() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.FORMATTED_RKF);
    new InstanceBuilder(fiji)
        .withTable(layout.getName(), layout)
            .withRow("dummy", "str1", "str2", 1, 2L)
                .withFamily("family").withQualifier("column")
                    .withValue(1L, "string-value")
                    .withValue(2L, "string-value2")
            .withRow("dummy", "str1", "str2", 1)
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
            .withRow("dummy", "str1", "str2")
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
            .withRow("dummy", "str1")
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
            .withRow("dummy")
                .withFamily("family").withQualifier("column").withValue(1L, "string-value")
        .build();

    final FijiTable table = fiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(), table.getURI().toString()));
      assertEquals(15, mToolOutputLines.length);
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testFijiScanStartAndLimitRow() throws Exception {
    final Fiji fiji = getFiji();

    TableLayoutDesc desc = FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
    ((RowKeyFormat2)desc.getKeysFormat()).setEncoding(RowKeyEncoding.RAW);

    final FijiTableLayout layout =  FijiTableLayout.newLayout(desc);
    final long timestamp = 10L;
    new InstanceBuilder(fiji)
        .withTable(layout.getName(), layout)
            .withRow("gwu@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "gwu@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Garrett Wu")
            .withRow("aaron@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "aaron@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Aaron Kimball")
            .withRow("christophe@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email")
                        .withValue(timestamp, "christophe@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Christophe Bisciglia")
            .withRow("kiyan@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "kiyan@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Kiyan Ahmadizadeh")
            .withRow("john.doe@gmail.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "john.doe@gmail.com")
                    .withQualifier("name").withValue(timestamp, "John Doe")
            .withRow("jane.doe@gmail.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "jane.doe@gmail.com")
                    .withQualifier("name").withValue(timestamp, "Jane Doe")
        .build();

    final FijiTable table = fiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(),
          table.getURI().toString() + "info:name"
      ));
      assertEquals(18, mToolOutputLines.length);

      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(),
              table.getURI().toString() + "info:name,info:email"
          ));
      assertEquals(30, mToolOutputLines.length);

        EntityIdFactory eif = EntityIdFactory.getFactory(layout);
        EntityId startEid = eif.getEntityId("christophe@usermail.example.com"); //second row
        EntityId limitEid = eif.getEntityId("john.doe@gmail.com"); //second to last row
        String startHbaseRowKey = Hex.encodeHexString(startEid.getHBaseRowKey());
        String limitHbaseRowKey = Hex.encodeHexString(limitEid.getHBaseRowKey());
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(),
          table.getURI().toString() + "info:name",
          "--start-row=hbase=hex:" + startHbaseRowKey,  // start at the second row.
          "--limit-row=hbase=hex:" + limitHbaseRowKey   // end at the 2nd to last row (exclusive).
      ));
      assertEquals(9, mToolOutputLines.length);

    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testRangeScanFormattedRKF() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.FORMATTED_RKF);
    new InstanceBuilder(fiji)
        .withTable(layout.getName(), layout)
            .withRow("NYC", "Technology", "widget", 1, 2)
                .withFamily("family").withQualifier("column")
                    .withValue("Candaules")
            .withRow("NYC", "Technology", "widget", 1, 20)
                .withFamily("family").withQualifier("column")
                    .withValue("Croesus")
            .withRow("NYC", "Technology", "thingie", 2)
                .withFamily("family").withQualifier("column")
                    .withValue("Gyges")
            .withRow("DC", "Technology", "stuff", 123)
                .withFamily("family").withQualifier("column")
                    .withValue("Glaucon")
            .withRow("DC", "Technology", "stuff", 124, 1)
                .withFamily("family").withQualifier("column")
                    .withValue("Lydia")
        .build();

    final FijiTable table = fiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(), table.getURI().toString(),
          "--start-row=['NYC','Technology','widget',1,2]",
          "--limit-row=['NYC','Technology','widget',1,30]",
          "--debug"
          ));
      assertEquals(10, mToolOutputLines.length);
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(), table.getURI().toString(),
          "--start-row=['NYC','Technology','widget']",
          "--limit-row=['NYC','Technology','widget',1,20]",
          "--debug"
          ));
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testTableNoFamilies() throws Exception {
    final Fiji fiji = new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.NOFAMILY))
        .build();
    final FijiTable table = fiji.openTable("nofamily");
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new ScanTool(), table.getURI().toString()));
    } finally {
      table.release();
    }
  }
}
