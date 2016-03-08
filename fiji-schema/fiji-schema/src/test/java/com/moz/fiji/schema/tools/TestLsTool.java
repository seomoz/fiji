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

package com.moz.fiji.schema.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestLsTool extends FijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLsTool.class);

  /**
   * Tests for the parsing of Fiji instance names from HBase table names for the
   * LsTool --instances functionality.
   */
  @Test
  public void testParseInstanceName() {
    assertEquals("default", LsTool.parseInstanceName("fiji.default.meta"));
    assertEquals("default", LsTool.parseInstanceName("fiji.default.schema_hash"));
    assertEquals("default", LsTool.parseInstanceName("fiji.default.schema_id"));
    assertEquals("default", LsTool.parseInstanceName("fiji.default.system"));
    assertEquals("default", LsTool.parseInstanceName("fiji.default.table.job_history"));
  }

  @Test
  public void testParseNonFijiTable() {
    String tableName = "hbase_table";
    assertEquals(null, LsTool.parseInstanceName(tableName));
  }

  @Test
  public void testParseMalformedFijiTableName() {
    assertEquals(null, LsTool.parseInstanceName("fiji.default"));
    assertEquals(null, LsTool.parseInstanceName("fiji."));
    assertEquals(null, LsTool.parseInstanceName("fiji"));
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testListInstances() throws Exception {
    final Fiji fiji = getFiji();
    final FijiURI hbaseURI = FijiURI.newBuilder(fiji.getURI()).withInstanceName(null).build();

    final LsTool ls = new LsTool();
    assertEquals(BaseTool.SUCCESS, runTool(ls, hbaseURI.toString()));
    final Set<String> instances = Sets.newHashSet(mToolOutputLines);
    assertTrue(instances.contains(fiji.getURI().toString()));
  }

  @Test
  public void testListTables() throws Exception {
    final Fiji fiji = getFiji();

    assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), fiji.getURI().toString()));
    assertEquals(1, mToolOutputLines.length);

    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.SIMPLE);
    fiji.createTable(layout.getDesc());

    assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), fiji.getURI().toString()));
    assertEquals(1, mToolOutputLines.length);
    assertEquals(fiji.getURI() + layout.getName(), mToolOutputLines[0]);
  }

  @Test
  public void testTableColumns() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.SIMPLE);
    fiji.createTable(layout.getDesc());
    final FijiTable table = fiji.openTable(layout.getName());
    try {
      // Table is empty:
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
      assertEquals(1, mToolOutputLines.length);
      assertTrue(mToolOutputLines[0].contains("family:column"));
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
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testFijiLsStartAndLimitRow() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayouts.getTableLayout(FijiTableLayouts.FOO_TEST);
    fiji.createTable(layout.getDesc());
    final FijiTable table = fiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
      // TODO: Validate output
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }


  @Test
  public void testMultipleArguments() throws Exception {
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

    final FijiTableLayout layoutTwo = FijiTableLayouts.getTableLayout(FijiTableLayouts.FOO_TEST);
    fiji.createTable(layoutTwo.getDesc());

    final FijiTable table = fiji.openTable(layout.getName());
    try {
      final FijiTable tableTwo = fiji.openTable(layoutTwo.getName());
      try {
        assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString(),
            tableTwo.getURI().toString()));
        assertEquals(9, mToolOutputLines.length);
        assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), fiji.getURI().toString()));
        assertEquals(2, mToolOutputLines.length);
        assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), fiji.getURI().toString(),
            table.getURI().toString()));
        assertEquals(3, mToolOutputLines.length);
      } finally {
        tableTwo.release();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testTableNoFamilies() throws Exception {
    final Fiji fiji = new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.NOFAMILY))
        .build();
    final FijiTable table = fiji.openTable("nofamily");
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new LsTool(), table.getURI().toString()));
    } finally {
      table.release();
    }
  }
}
