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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.filter.FijiColumnRangeFilter;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestHBaseMapFamilyPager extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseMapFamilyPager.class);

  private FijiTableReader mReader;
  private FijiTable mTable;

  private static final int NJOBS = 5;
  private static final long NTIMESTAMPS = 5;

  @Before
  public final void setupTestHBaseMapFamilyPager() throws Exception {
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
  public final void teardownTestHBaseMapFamilyPager() throws IOException {
    mReader.close();
    mTable.release();
  }

  /** Test a qualifier pager on a map-type family with no user filter. */
  @Test
  public void testQualifiersPager() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int pageSize = 2;
    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(HConstants.ALL_VERSIONS).withPageSize(pageSize).addFamily("jobs"))
        .build();

    final FijiRowData row = mReader.get(eid, dataRequest);
    final FijiPager pager = row.getPager("jobs");
    try {
      assertTrue(pager.hasNext());

      final List<String> qualifiers = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        // Note: we cannot validate individual pages as their boundaries depend on HBase version.
        final NavigableSet<String> page = pager.next().getQualifiers("jobs");
        LOG.info("Page #{}: {}", npage, page);
        assertTrue(page.size() <= pageSize);
        qualifiers.addAll(page);
        npage++;
      }

      final List<String> expected = Lists.newArrayList("j0", "j1", "j2", "j3", "j4");
      assertTrue(npage >= 3);  // at least 3 pages
      assertEquals(expected, qualifiers);

      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test a qualifier pager on a map-type family with a user filter that discards everything. */
  @Test
  public void testQualifiersPagerWithUserFilterEmpty() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int pageSize = 2;
    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(pageSize)
            .withFilter(new FijiColumnRangeFilter("j12", true, "j13", true))
            .addFamily("jobs"))
        .build();

    final FijiRowData row = mReader.get(eid, dataRequest);
    final FijiPager pager = row.getPager("jobs");
    try {
      // The fiji pager API specifies that the pager may return true, in which case the resulting
      // FijiRowData must be empty.
      if (pager.hasNext()) {
        assertTrue(pager.next().getQualifiers("jobs").isEmpty());
        assertFalse(pager.hasNext());
      }
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test a qualifier pager on a map-type family with a user filter. */
  @Test
  public void testQualifiersPagerWithUserFilter() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int pageSize = 2;
    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(pageSize)
            .withFilter(new FijiColumnRangeFilter("j1", true, "j2", true))
            .addFamily("jobs"))
        .build();

    final FijiRowData row = mReader.get(eid, dataRequest);
    final FijiPager pager = row.getPager("jobs");
    try {
      assertTrue(pager.hasNext());

      final List<String> qualifiers = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        // Note: we cannot validate individual pages as their boundaries depend on HBase version.
        final NavigableSet<String> page = pager.next().getQualifiers("jobs");
        LOG.info("Page #{}: {}", npage, page);
        assertTrue(page.size() <= pageSize);
        qualifiers.addAll(page);
        npage++;
      }

      final List<String> expected = Lists.newArrayList("j1", "j2");
      assertTrue(npage >= 1);  // at least 1 page
      assertEquals(expected, qualifiers);

      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test a qualifier pager on a map-type family with a user filter and several pages. */
  @Test
  public void testQualifiersPagerWithUserFilter2() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int pageSize = 2;
    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(pageSize)
            .withFilter(new FijiColumnRangeFilter("j1", true, null, true))
            .addFamily("jobs"))
        .build();

    final FijiRowData row = mReader.get(eid, dataRequest);
    final FijiPager pager = row.getPager("jobs");
    try {
      assertTrue(pager.hasNext());

      final List<String> qualifiers = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        // Note: we cannot validate individual pages as their boundaries depend on HBase version.
        final NavigableSet<String> page = pager.next().getQualifiers("jobs");
        LOG.info("Page #{}: {}", npage, page);
        assertTrue(page.size() <= pageSize);
        qualifiers.addAll(page);
        npage++;
      }

      final List<String> expected = Lists.newArrayList("j1", "j2", "j3", "j4");
      assertTrue(npage >= 2);  // at least 2 pages
      assertEquals(expected, qualifiers);

      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test illustrating how paging through all cells in a map-type family works. */
  @Test
  public void testFullMapPaging() throws Exception {
    final EntityId eid = mTable.getEntityId("me");

    final int qualifiersPageSize = 3;
    final int versionsPageSize = 3;

    final FijiDataRequest qualifiersDataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(qualifiersPageSize)
            .addFamily("jobs"))
        .build();

    final FijiRowData qualifiersRow = mReader.get(eid, qualifiersDataRequest);
    final FijiPager qualifiersPager = qualifiersRow.getPager("jobs");
    try {
      int qualifiersCounter = 0;

      while (qualifiersPager.hasNext()) {
        final FijiRowData qualifierPage = qualifiersPager.next();
        LOG.debug("New qualifier page with {} qualifiers",
            qualifierPage.getQualifiers("jobs").size());
        for (String qualifier : qualifierPage.getQualifiers("jobs")) {
          qualifiersCounter += 1;
          int versionsCounter = 0;

          final FijiDataRequest versionsDataRequest = FijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withPageSize(versionsPageSize)
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("jobs", qualifier))
              .build();
          final FijiRowData versionsRow = mReader.get(eid, versionsDataRequest);
          final FijiPager versionsPager = versionsRow.getPager("jobs", qualifier);
          try {
            while (versionsPager.hasNext()) {
              final FijiRowData versionsPage = versionsPager.next();
              LOG.debug("New version page with {} versions",
                  versionsPage.getValues("jobs", qualifier).size());
              for (Map.Entry<Long, String> entry
                  : versionsPage.<String>getValues("jobs", qualifier).entrySet()) {
                versionsCounter += 1;
                LOG.debug("Entry: {} -> {}", entry.getKey(), entry.getValue());
              }
            }

            assertEquals(NTIMESTAMPS, versionsCounter);
          } finally {
            versionsPager.close();
          }
        }
      }

      assertEquals(NJOBS, qualifiersCounter);
    } finally {
      qualifiersPager.close();
    }
  }
}
