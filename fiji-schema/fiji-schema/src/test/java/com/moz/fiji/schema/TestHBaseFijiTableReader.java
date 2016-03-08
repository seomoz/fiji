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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestHBaseFijiTableReader extends FijiClientTest {
  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;

  @Before
  public final void setupEnvironment() throws Exception {
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
                    .withQualifier("name").withValue(1L, "bar-val")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mFiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws Exception {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testGet() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("info", "name");
    final FijiDataRequest request = builder.build();
    final String actual = mReader.get(entityId, request).getValue("info", "name", 1L).toString();
    assertEquals("foo-val", actual);
  }

  @Test
  public void testGetCounter() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final FijiDataRequest request = FijiDataRequest.create("info", "visits");
    FijiCell<Long> counter = mReader.get(entityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(42L, actual);
  }

  @Test
  public void testBulkGet() throws Exception {
    final List<EntityId> entityIds =
        Lists.newArrayList(mTable.getEntityId("foo"), mTable.getEntityId("bar"));
    final FijiDataRequest request = FijiDataRequest.create("info", "name");
    final List<FijiRowData> data = mReader.bulkGet(entityIds, request);

    final String actual1 = data.get(0).getValue("info", "name", 1L).toString();
    final String actual2 = data.get(1).getValue("info", "name", 1L).toString();
    assertEquals("foo-val", actual1);
    assertEquals("bar-val", actual2);
  }

  @Test
  public void testBulkGetResults() throws IOException {
    final List<EntityId> entityIds =
        Lists.newArrayList(mTable.getEntityId("foo"), mTable.getEntityId("bar"));
    final FijiDataRequest request = FijiDataRequest.create("info", "name");
    final List<FijiResult<Utf8>> data = mReader.bulkGetResults(entityIds, request);

    final String actual1 = data.get(0).iterator().next().getData().toString();
    final String actual2 = data.get(1).iterator().next().getData().toString();
    assertEquals("foo-val", actual1);
    assertEquals("bar-val", actual2);
  }
}
