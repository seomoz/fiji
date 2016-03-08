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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestFijiAdmin extends FijiClientTest {
  private static final String LAYOUT_V1 = FijiTableLayouts.SIMPLE_FORMATTED_EID;

  /** Layout update on top of LAYOUT_V1 with an extra locality group called 'new'. */
  private static final String LAYOUT_V2 =
      "com.moz.fiji/schema/layout/simple-update-new-locality-group.json";

  private TableLayoutDesc mLayoutDesc;
  private TableLayoutDesc mLayoutDescUpdate;

  private FijiTableLayout getLayout(String table) throws IOException {
    return getFiji().getMetaTable().getTableLayout(table);
  }

  @Before
  public void setup() throws IOException {
    mLayoutDesc = FijiTableLayouts.getLayout(LAYOUT_V1);
    mLayoutDescUpdate = FijiTableLayouts.getLayout(LAYOUT_V2);
  }

  // -----------------------------------------------------------------------------------------------

  /** Tests that creating a new table works fine. */
  @Test
  public void testCreateTable() throws Exception {
    getFiji().createTable(mLayoutDesc);
    assertEquals(mLayoutDesc.getName(), getLayout("table").getName());
  }

  /** Tests a layout update that adds a locality group with a column. */
  @Test
  public void testSetTableLayoutAdd() throws Exception {
    getFiji().createTable(mLayoutDesc);

    mLayoutDescUpdate.setReferenceLayout(getLayout("table").getDesc().getLayoutId());

    final FijiTableLayout tableLayout = getFiji().modifyTableLayout(mLayoutDescUpdate);
    assertEquals(tableLayout.getFamilies().size(), getLayout("table").getFamilies().size());
  }

  @Test
  public void testSetTableLayoutModify() throws Exception {
    getFiji().createTable(mLayoutDesc);

    final TableLayoutDesc newTableLayoutDesc = TableLayoutDesc.newBuilder(mLayoutDesc).build();
    assertEquals(3, (int) newTableLayoutDesc.getLocalityGroups().get(0).getMaxVersions());
    newTableLayoutDesc.getLocalityGroups().get(0).setMaxVersions(1);
    newTableLayoutDesc.setReferenceLayout(getLayout("table").getDesc().getLayoutId());

    final FijiTableLayout newTableLayout = getFiji().modifyTableLayout(newTableLayoutDesc);
    assertEquals(
        newTableLayout.getLocalityGroupMap().get("default").getDesc().getMaxVersions(),
        getLayout("table").getLocalityGroupMap().get("default").getDesc().getMaxVersions());
  }

  @Test
  public void testDeleteTable() throws Exception {
    getFiji().createTable(mLayoutDesc);
    assertNotNull(getLayout("table"));

    getFiji().deleteTable("table");

    // Make sure it was deleted from the meta table, too.
    // The following line should throw a FijiTableNotFoundException.
    try {
      getLayout("table");
      fail("An exception should have been thrown.");
    } catch (FijiTableNotFoundException ktnfe) {
      assertTrue(ktnfe.getMessage().startsWith("FijiTable not found: fiji://"));
      assertEquals("table", ktnfe.getTableURI().getTable());
    }
  }

  @Test
  public void testSetTableLayoutOnATableThatDoesNotExist() throws Exception {
    final TableLayoutDesc tableLayoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    try {
      getFiji().modifyTableLayout(tableLayoutDesc);
      fail("An exception should have been thrown.");
    } catch (FijiTableNotFoundException ktnfe) {
      assertTrue(ktnfe.getMessage().startsWith("FijiTable not found: fiji://"));
      assertEquals("table", ktnfe.getTableURI().getTable());
    }
  }
}
