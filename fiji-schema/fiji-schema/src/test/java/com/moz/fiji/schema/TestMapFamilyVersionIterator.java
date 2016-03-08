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

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.MapFamilyVersionIterator.Entry;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestMapFamilyVersionIterator extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestMapFamilyVersionIterator.class);

  private FijiTableReader mReader;
  private FijiTable mTable;

  private static final int NJOBS = 5;
  private static final long NTIMESTAMPS = 5;

  @Before
  public final void setupTestFijiPager() throws Exception {
    final Fiji fiji = getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.PAGING_TEST));

    mTable = fiji.openTable("user");
    final EntityId eid = mTable.getEntityId("me");
    final FijiTableWriter writer = mTable.openTableWriter();
    try {
      for (int job = 0; job < NJOBS; ++job) {
        for (long ts = 1; ts <= NTIMESTAMPS; ++ts) {
          writer.put(eid, "jobs", String.format("j%d", job), ts, String.format("j%d-t%d", job, ts));
        }
      }
    } finally {
      writer.close();
    }

    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestFijiPager() throws IOException {
    mReader.close();
    mTable.release();
  }

  /** Test a qualifier iterator. */
  @Test
  public void testQualifiersIterator() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(HConstants.ALL_VERSIONS).withPageSize(1).addFamily("jobs"))
        .build();

    final FijiRowData row = mReader.get(eid, dataRequest);
    final MapFamilyVersionIterator<Utf8> it =
        new MapFamilyVersionIterator<Utf8>(row, "jobs", 3, 2);
    try {
      int ncells = 0;
      int ijob = 0;
      int timestamp = 5;
      for (Entry<Utf8> entry : it) {
        assertEquals(String.format("j%d", ijob), entry.getQualifier());
        assertEquals(timestamp, entry.getTimestamp());
        assertEquals(String.format("j%d-t%d", ijob, timestamp), entry.getValue().toString());
        timestamp -= 1;
        if (timestamp == 0) {
          timestamp = 5;
          ijob += 1;
        }
        ncells += 1;
      }
      assertEquals(NJOBS * NTIMESTAMPS, ncells);
    } finally {
      it.close();
    }
  }
}
