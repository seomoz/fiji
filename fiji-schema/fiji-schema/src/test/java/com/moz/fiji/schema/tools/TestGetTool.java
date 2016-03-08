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
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestGetTool extends FijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGetTool.class);

  @Test
  public void testUnderspecified() throws Exception {
    final Fiji fiji = getFiji();
    final FijiURI hbaseURI = FijiURI.newBuilder(fiji.getURI()).withInstanceName(null).build();

    assertEquals(BaseTool.FAILURE, runTool(new GetTool(), hbaseURI.toString()));
    assertTrue(mToolOutputLines[0].startsWith("Specify a cluster"));
    assertEquals(BaseTool.FAILURE, runTool(new GetTool()));
    assertTrue(mToolOutputLines[0].startsWith("URI must be specified"));
    assertEquals(BaseTool.FAILURE, runTool(new GetTool(),
        hbaseURI.toString() + "instance/table",
        "--max-versions=0"));
    assertTrue(mToolOutputLines[0].startsWith("--max-versions must be positive"));
  }

  @Test
  public void testGetFromTable() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.SIMPLE);
    fiji.createTable(layout.getName(), layout);

    new InstanceBuilder(fiji)
        .withTable(layout.getName(), layout)
            .withRow("hashed")
                .withFamily("family").withQualifier("column").withValue(314L, "value")
        .build();

    final FijiTable table = fiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=hashed"));
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testGetFromTableMore() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.FOO_TEST);
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
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=[\"jane.doe@gmail.com\"]"));
      assertEquals(5, mToolOutputLines.length);
      EntityId eid = EntityIdFactory.getFactory(layout).getEntityId("gwu@usermail.example.com");
      String hbaseRowKey = Hex.encodeHexString(eid.getHBaseRowKey());
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(),
          table.getURI() + "info:name",
              "--entity-id=hbase=hex:" + hbaseRowKey
          ));
      assertEquals(3, mToolOutputLines.length);
      // TODO: Validate GetTool output
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testGetFormattedRKF() throws Exception {
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
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=['NYC','Technology','widget',1,2]"
          ));
      assertEquals(3, mToolOutputLines.length);
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=['NYC','Technology','thingie',2,null]"
          ));
      assertEquals(3, mToolOutputLines.length);
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }
}
