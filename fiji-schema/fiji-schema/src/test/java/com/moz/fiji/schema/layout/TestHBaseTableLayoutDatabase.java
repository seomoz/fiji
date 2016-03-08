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

package com.moz.fiji.schema.layout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.impl.hbase.HBaseMetaTable;
import com.moz.fiji.schema.layout.impl.HBaseTableLayoutDatabase;
import com.moz.fiji.schema.util.InstanceBuilder;


public class TestHBaseTableLayoutDatabase extends FijiClientTest {
  private FijiURI mFijiURI;
  private HBaseTableLayoutDatabase mTableLayoutDatabase;

  @Before
  public final void setupTest() throws IOException {
    final FijiSchemaTable schemaTable = getFiji().getSchemaTable();

    final FijiURI hbaseURI = createTestHBaseURI();
    final String instanceName =
        String.format("%s_%s", getClass().getSimpleName(), mTestName.getMethodName());
    mFijiURI = FijiURI.newBuilder(hbaseURI).withInstanceName(instanceName).build();
    final HBaseFactory factory = HBaseFactory.Provider.get();
    HBaseMetaTable.install(factory.getHBaseAdminFactory(mFijiURI).create(getConf()), mFijiURI);

    final HTableInterface htable =
        HBaseMetaTable.newMetaTable(
            mFijiURI, getConf(), factory.getHTableInterfaceFactory(mFijiURI));
    final String family = "layout";
    mTableLayoutDatabase = new HBaseTableLayoutDatabase(mFijiURI, htable, family, schemaTable);
  }

  @After
  public final void teardownTest() throws IOException {
    final HBaseFactory factory = HBaseFactory.Provider.get();
    HBaseMetaTable.uninstall(factory.getHBaseAdminFactory(mFijiURI).create(getConf()), mFijiURI);
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testSetLayout() throws Exception {
    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    final FijiTableLayout expected = FijiTableLayout.newLayout(layoutDesc);
    final FijiTableLayout result =
        mTableLayoutDatabase.updateTableLayout(layoutDesc.getName(), layoutDesc);
    assertEquals(expected, result);
  }

  @Test
  public void testGetLayout() throws Exception {
    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    final FijiTableLayout layout =
        mTableLayoutDatabase.updateTableLayout(layoutDesc.getName(), layoutDesc);

    final FijiTableLayout result = mTableLayoutDatabase.getTableLayout(layoutDesc.getName());
    assertEquals(layout, result);
  }

  @Test
  public void testTableExists() throws IOException {
    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    final String tableName = layoutDesc.getName();
    final Fiji fiji = new InstanceBuilder(getFiji())
        .withTable(layoutDesc)
        .build();
    assertTrue(fiji.getMetaTable().tableExists(tableName));
    assertFalse(fiji.getMetaTable().tableExists("faketablename"));
  }

  @Test
  public void testGetMultipleLayouts() throws Exception {
    final TableLayoutDesc layoutDesc1 = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    layoutDesc1.setVersion("layout-1.0");
    final String tableName = layoutDesc1.getName();
    final FijiTableLayout layout1 = mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc1);

    // This thread sleep prevents this value from overwriting the previous in case both writes occur
    // within the same millisecond.
    Thread.sleep(2);
    final TableLayoutDesc layoutDesc2 = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    layoutDesc2.setVersion("layout-1.0.1");
    layoutDesc2.setReferenceLayout(layout1.getDesc().getLayoutId());
    final FijiTableLayout layout2 = mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc2);

    Thread.sleep(2);
    final TableLayoutDesc layoutDesc3 = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    layoutDesc3.setVersion("layout-1.1");
    layoutDesc3.setReferenceLayout(layout2.getDesc().getLayoutId());
    final FijiTableLayout layout3 = mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc3);

    final NavigableMap<Long, FijiTableLayout> timeSeries =
        mTableLayoutDatabase.getTimedTableLayoutVersions(tableName, HConstants.ALL_VERSIONS);
    assertEquals(3, timeSeries.size());

    final List<FijiTableLayout> layouts = Lists.newArrayList(timeSeries.values());
    assertEquals(layout1, layouts.get(0));
    assertEquals(layout2, layouts.get(1));
    assertEquals(layout3, layouts.get(2));
  }

  /**
   * Layout IDs must be unique to guarantee that no race condition may occur when applying
   * table layout updates.
   */
  @Test
  public void testUniqueLayoutIDs() throws IOException {
    // Creates a table with a first layout:
    final TableLayoutDesc layoutDesc1 = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    layoutDesc1.setLayoutId("layout-ID");
    final String tableName = layoutDesc1.getName();
    mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc1);

    // Try applying a new layout with the same layout ID the current layout has:
    final TableLayoutDesc layoutDesc2 = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    layoutDesc2.setReferenceLayout("layout-ID");
    layoutDesc2.setLayoutId("layout-ID");
    try {
      // This update must fail due to the duplicate layout ID "layout-ID":
      mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc2);
      Assert.fail("layout2 should be invalid : its layout ID is not unique.");
    } catch (InvalidLayoutException ile) {
      // Expected:
      assertTrue(ile.getMessage().contains("Layout ID 'layout-ID' already exists"));
    }

    // Applies a new layout with a different ID:
    layoutDesc2.setLayoutId("layout2-ID");
    mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc2);

    // Finally, try applying a new layout with a layout ID from a former layout:
    final TableLayoutDesc layoutDesc3 = FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE);
    layoutDesc3.setReferenceLayout("layout-ID");
    layoutDesc3.setLayoutId("layout-ID");
    try {
      // This update must fail due to the duplicate layout ID "layout-ID":
      mTableLayoutDatabase.updateTableLayout(tableName, layoutDesc3);
      Assert.fail("layout3 should be invalid : its layout ID is not unique.");
    } catch (InvalidLayoutException ile) {
      // Expected:
      assertTrue(ile.getMessage().contains("Layout ID 'layout-ID' already exists"));
    }
  }

}
