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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestDeleteTool extends FijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeleteTool.class);

  /** Table used to test against. Owned by this test. */
  private FijiTable mTable = null;

  private FijiURI mTableURI;

  private FijiTableReader mReader = null;

  private FijiTableLayout mLayout;

  // -----------------------------------------------------------------------------------------------

  @Before
  public final void setupTestDeleteTool() throws Exception {
    mLayout = FijiTableLayouts.getTableLayout(FijiTableLayouts.SIMPLE);
    new InstanceBuilder(getFiji())
        .withTable(mLayout.getName(), mLayout)
            .withRow("row-1")
                .withFamily("family").withQualifier("column")
                    .withValue(313L, "value1")
                    .withValue(314L, "value2")
                    .withValue(315L, "value3")
        .build();
    mTableURI = FijiURI.newBuilder(getFiji().getURI()).withTableName(mLayout.getName()).build();
  }

  @After
  public final void teardownTestDeleteTool() throws Exception {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    mReader = null;
    mTable = null;
  }

  /**
   * Many of these tests should not have an open table connection, so only open it on demand.
   *
   * @throws Exception sometimes.
   */
  private void setupTableAndReader() throws Exception {
    mTable = getFiji().openTable(mLayout.getName());
    mReader = mTable.openTableReader();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testDeleteAllCellsInRow() throws Exception {
    setupTableAndReader();
    final FijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), FijiDataRequest.create("family"));
    assertTrue(rowBefore.containsColumn("family"));

    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + mTableURI,
      "--entity-id=row-1",
      "--interactive=false"
    ));

    final FijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), FijiDataRequest.create("family"));
    assertFalse(rowAfter.containsColumn("family"));
  }

  @Test
  public void testDeleteAllCellsInFamilyFromRow() throws Exception {
    setupTableAndReader();
    final FijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), FijiDataRequest.create("family"));
    assertTrue(rowBefore.containsColumn("family"));

    // Target one column family:
    final FijiURI target =
        FijiURI.newBuilder(mTableURI).addColumnName(FijiColumnName.create("family")).build();

    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--interactive=false"
    ));

    final FijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), FijiDataRequest.create("family"));
    assertFalse(rowAfter.containsColumn("family"));
  }

  @Test
  public void testDeleteAllCellsInColumnFromRow() throws Exception {
    setupTableAndReader();
    final FijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), FijiDataRequest.create("family"));
    assertTrue(rowBefore.containsColumn("family"));

    // Target one column:
    final FijiURI target = FijiURI.newBuilder(mTableURI)
        .addColumnName(FijiColumnName.create("family", "column"))
        .build();

    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--interactive=false"
    ));

    final FijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), FijiDataRequest.create("family"));
    assertFalse(rowAfter.containsColumn("family"));
  }

  @Test
  public void testDeleteMostRecentCellInColumnFromRow() throws Exception {
    setupTableAndReader();
    final FijiDataRequestBuilder kdrb = FijiDataRequest.builder();
    kdrb.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
        .add("family", "column");
    final FijiDataRequest kdr = kdrb.build();
    final FijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(3, rowBefore.getValues("family", "column").size());
    assertEquals(315L, (long) rowBefore.getValues("family", "column").firstKey());

    // Target one column:
    final FijiURI target = FijiURI.newBuilder(mTableURI)
        .addColumnName(FijiColumnName.create("family", "column"))
        .build();

    // Delete cells with latest timestamp, ie. timestamp == 315
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--timestamp=latest",
      "--interactive=false"
    ));

    final FijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(2, rowAfter.getValues("family", "column").size());
    assertEquals(314L, (long) rowAfter.getValues("family", "column").firstKey());
  }

  @Test
  public void testDeleteExactTimestampCellInColumnFromRow() throws Exception {
    setupTableAndReader();
    final FijiDataRequestBuilder kdrb = FijiDataRequest.builder();
    kdrb.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
        .add("family", "column");
    final FijiDataRequest kdr = kdrb.build();
    final FijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(3, rowBefore.getValues("family", "column").size());
    assertEquals(315L, (long) rowBefore.getValues("family", "column").firstKey());

    // Target one column:
    final FijiURI target = FijiURI.newBuilder(mTableURI)
        .addColumnName(FijiColumnName.create("family", "column"))
        .build();

    // Delete cells with timestamp == 314
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--timestamp=314",
      "--interactive=false"
    ));

    final FijiRowData rowAfter = mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(2, rowAfter.getValues("family", "column").size());
    assertEquals(315L, (long) rowAfter.getValues("family", "column").firstKey());
    assertEquals(313L, (long) rowAfter.getValues("family", "column").lastKey());
  }

  @Test
  public void testDeleteUpToTimestampCellInColumnFromRow() throws Exception {
    setupTableAndReader();
    final FijiDataRequestBuilder kdrb = FijiDataRequest.builder();
    kdrb.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
        .add("family", "column");
    final FijiDataRequest kdr = kdrb.build();
    final FijiRowData rowBefore =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(3, rowBefore.getValues("family", "column").size());
    assertEquals(315L, (long) rowBefore.getValues("family", "column").firstKey());

    // Target one column:
    final FijiURI target = FijiURI.newBuilder(mTableURI)
        .addColumnName(FijiColumnName.create("family", "column"))
        .build();

    // Delete cells with timestamps <= 314
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
      "--target=" + target,
      "--entity-id=row-1",
      "--timestamp=upto:314",
      "--interactive=false"
    ));

    final FijiRowData rowAfter =
        mReader.get(mTable.getEntityId("row-1"), kdr);
    assertEquals(1, rowAfter.getValues("family", "column").size());
    assertEquals(315L, (long) rowAfter.getValues("family", "column").firstKey());
  }

  @Test
  public void testInteractiveWithWrongInstanceInput() throws Exception {
    assertEquals(BaseTool.FAILURE, runToolWithInput(new DeleteTool(),
        "wrongtable",
        "--target=" + mTableURI
    ));
  }

  @Test
  public void testInteractiveWithTableInput() throws Exception {
    assertEquals(BaseTool.SUCCESS, runToolWithInput(new DeleteTool(),
        "table",
        "--target=" + mTableURI
    ));
  }

  @Test
  public void testInteractiveWithWrongTableInput() throws Exception {
    assertEquals(BaseTool.FAILURE, runToolWithInput(new DeleteTool(),
        "wrongtable",
        "--target=" + mTableURI
    ));
  }

  @Test
  public void testNoninteractiveTableDelete() throws Exception {
    assertEquals(BaseTool.SUCCESS, runTool(new DeleteTool(),
        "--target=" + mTableURI,
        "--interactive=false"
    ));
  }
}
