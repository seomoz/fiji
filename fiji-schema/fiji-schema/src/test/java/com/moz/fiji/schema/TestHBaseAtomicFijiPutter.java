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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestHBaseAtomicFijiPutter extends FijiClientTest {
  private Fiji mFiji;
  private FijiTable mTable;
  private AtomicFijiPutter mPutter;
  private FijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout = FijiTableLayout.newLayout(
        FijiTableLayouts.getLayout(FijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mFiji = new InstanceBuilder(getFiji())
        .withTable("user", layout)
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name").withValue(1L, "foo-val")
                    .withQualifier("visits").withValue(1L, 42L)
            .withRow("bar")
                .withFamily("info")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mFiji.openTable("user");
    mPutter = mTable.getWriterFactory().openAtomicPutter();
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    mPutter.close();
    mReader.close();
    mTable.release();
  }

  @Test
  public void testBasicPut() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest request = FijiDataRequest.create("info", "visits");

    mPutter.begin(eid);
    assertEquals(42L,
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData());
    mPutter.put("info", "visits", 45L);
    assertEquals(42L,
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData());
    mPutter.commit();
    assertEquals(45L,
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData());
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest request = FijiDataRequest.create("info", "visits");

    assertEquals(42L,
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData());

    mPutter.begin(eid);
    mPutter.put("info", "visits", 10L, 45L);
    mPutter.commit();

    assertEquals(45L,
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData());

    mPutter.begin(eid);
    mPutter.put("info", "visits", 5L, 50L);
    mPutter.commit();

    assertEquals(45L,
        mReader.get(eid, request).getMostRecentCell("info", "visits").getData());
  }

  @Test
  public void testCompoundPut() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest nameRequest = FijiDataRequest.create("info", "name");
    final FijiDataRequest visitsRequest = FijiDataRequest.create("info", "visits");

    assertEquals("foo-val",
        mReader.get(eid, nameRequest).getMostRecentCell("info", "name").getData().toString());
    assertEquals(42L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());

    mPutter.begin(eid);
    mPutter.put("info", "name", "foo-name");
    mPutter.put("info", "visits", 45L);
    mPutter.commit();

    assertEquals("foo-name",
        mReader.get(eid, nameRequest).getMostRecentCell("info", "name").getData().toString());
    assertEquals(45L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());
  }

  @Test
  public void testBasicCheckandCommit() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest visitsRequest = FijiDataRequest.create("info", "visits");

    assertEquals(42L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());

    mPutter.begin(eid);
    mPutter.put("info", "visits", 45L);
    assertFalse(mPutter.checkAndCommit("info", "visits", 43L));

    assertEquals(42L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());

    assertTrue(mPutter.checkAndCommit("info", "visits", 42L));

    assertEquals(45L,
        mReader.get(eid, visitsRequest).getMostRecentCell("info", "visits").getData());
  }

  @Test
  public void testCheckNullAndCommit() throws Exception {
    final EntityId eid = mTable.getEntityId("dynasty");
    final FijiDataRequest nameRequest = FijiDataRequest.create("info", "name");

    assertEquals(null, mReader.get(eid, nameRequest).getMostRecentCell("info", "name"));

    mPutter.begin(eid);
    mPutter.put("info", "name", "valois");
    assertFalse(mPutter.checkAndCommit("info", "name", "henri"));

    assertEquals(null, mReader.get(eid, nameRequest).getMostRecentCell("info", "name"));

    assertTrue(mPutter.checkAndCommit("info", "name", null));

    assertEquals("valois",
        mReader.get(eid, nameRequest).getMostRecentValue("info", "name").toString());
  }

  @Test
  public void testSkipBegin() throws Exception {
    try {
      mPutter.put("info", "visits", 45L);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("calls to put() must be between calls to begin() and "
          + "commit(), checkAndCommit(), or rollback()", ise.getMessage());
    }
  }

  @Test
  public void testBeginTwice() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");

    mPutter.begin(eid);
    try {
      mPutter.begin(eid);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("There is already a transaction in progress on row: hbase=foo. "
          + "Call commit(), checkAndCommit(), or rollback() to clear the Put.", ise.getMessage());
    }
  }

  @Test
  public void testRollback() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");

    mPutter.begin(eid);
    mPutter.put("info", "visits", 45L);
    mPutter.put("info", "name", "foo-name");
    mPutter.rollback();

    assertEquals(null, mPutter.getEntityId());
    try {
      mPutter.commit();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("commit() must be paired with a call to begin()", ise.getMessage());
    }
  }
}
