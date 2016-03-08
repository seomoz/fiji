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

package com.moz.fiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.impl.hbase.HBaseFijiTable;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestFijiTable extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFijiTable.class);

  @Test
  public void testGetRegions() throws IOException {
    final int numRegions = 3;

    final Fiji mFiji = getFiji();
    mFiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FULL_FEATURED), numRegions);
    final FijiTable mTable = mFiji.openTable("user");
    try {
      // Check there are the right number of regions.
      List<FijiRegion> regions = mTable.getRegions();
      assertEquals(numRegions, regions.size());

      // Check that all FijiRegions have location info.
      for (FijiRegion region : regions) {
        assertTrue(region.getLocations().size() > 0);
      }
    } finally {
      mTable.release();
    }
  }

  @Test
  public void testGetRegionsThenAnotherCommand() throws IOException {
    // SCHEMA-258: getRegions() would close the table, so you can't do more work with it.
    final int numRegions = 3;

    final Fiji mFiji = getFiji();
    mFiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FULL_FEATURED), numRegions);
    final FijiTable mTable = mFiji.openTable("user");
    LOG.info("Opened Fiji table: {}", mTable);
    assertTrue("This isn't testing HBaseFijiTable, it's " + mTable.getClass().getName(),
        mTable instanceof HBaseFijiTable);
    try {
      mTable.getRegions();

      // Try to get the region list again; make sure this doesn't explode.
      mTable.getRegions();

      FijiTableWriter writer = mTable.openTableWriter();
      writer.put(mTable.getEntityId("username"), "info", "name", "Bob");
      writer.close();

      FijiTableReader reader = mTable.openTableReader();
      reader.get(mTable.getEntityId("username"), FijiDataRequest.create("info", "name"));
      reader.close();
    } finally {
      mTable.release();
    }
  }
}
