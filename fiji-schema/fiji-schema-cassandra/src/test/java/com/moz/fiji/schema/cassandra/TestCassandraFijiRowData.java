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

package com.moz.fiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestCassandraFijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraFijiRowData.class);

  /** Test layout. */
  public static final String TEST_LAYOUT_V1 =
      "com.moz.fiji/schema/layout/TestHBaseFijiRowData.test-layout-v1.json";

  private static final String TABLE_NAME = "row_data_test_table";

  private static final String FAMILY = "family";
  private static final String EMPTY = "empty";
  private static final String QUAL0 = "qual0";
  private static final String QUAL3 = "qual3";
  private static final String MAP = "map";
  private static final String KEY0 = "key0";
  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraFijiClientTest CLIENT_TEST_DELEGATE = new CassandraFijiClientTest();

  /** FijiTable used for some tests (named TABLE_NAME). */
  private static FijiTable mTable;

  /** Unique per test case -- keep tests on different rows. */
  private EntityId mEntityId;
  private FijiTableReader mReader;
  private FijiTableWriter mWriter;

  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupFijiTest();
    Fiji fiji = CLIENT_TEST_DELEGATE.getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    mTable = fiji.openTable(TABLE_NAME);
  }

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + TEST_ID_COUNTER.getAndIncrement());
  }

  @After
  public final void tearDownTestHBaseFijiRowData() throws Exception {
    mReader.close();
    mWriter.close();
  }

  @AfterClass
  public static void closeOut() throws Exception {
    mTable.release();
    CLIENT_TEST_DELEGATE.tearDownFijiTest();
  }

  // -----------------------------------------------------------------------------------------------
  // Test cases that need to interact with an actual Fiji table.

  @Test
  public void testEntityId() throws Exception {
    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(FAMILY, QUAL0))
        .build();

    // Put some data into the table.
    mWriter.put(mEntityId, FAMILY, QUAL0, "bot");

    // Read out the results to get a FijiRowData
    final FijiRowData input = mReader.get(mEntityId, dataRequest);
    assertEquals(mEntityId, input.getEntityId());
  }

  @Test
  public void testReadInts() throws Exception {
    // Put some data into the table.
    mWriter.put(mEntityId, FAMILY, QUAL3, 1L, 42);

    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add(FAMILY, QUAL3);
    FijiDataRequest dataRequest = builder.build();

    // Read out the results to get a FijiRowData
    final FijiRowData input = mReader.get(mEntityId, dataRequest);

    assertNotNull(input.getMostRecentValue(FAMILY, QUAL3));
    final int integer = input.getMostRecentValue(FAMILY, QUAL3);
    assertEquals(42, integer);
  }

  @Test
  public void testGetReaderSchema() throws Exception {
    // Empty data request.
    final FijiDataRequest dataRequest = FijiDataRequest.builder().build();

    // Read data for an entity ID that does not exist.
    final FijiRowData input = mReader.get(mEntityId, dataRequest);

    assertEquals(Schema.create(Schema.Type.STRING), input.getReaderSchema("family", "empty"));
    assertEquals(Schema.create(Schema.Type.INT), input.getReaderSchema("family", "qual3"));
  }

  @Test
  public void testGetReaderSchemaNoSuchColumn() throws Exception {
    final FijiDataRequest dataRequest = FijiDataRequest.builder().build();

    // Read data for an entity ID that does not exist.
    final FijiRowData input = mReader.get(mEntityId, dataRequest);

    try {
      input.getReaderSchema("this_family", "does_not_exist");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no family 'this_family'.", nsce.getMessage());
    }

    try {
      input.getReaderSchema("family", "no_qualifier");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no column 'family:no_qualifier'.",
          nsce.getMessage());
    }
  }

  @Test
  public void testReadMiddleTimestamp() throws IOException {
    // Test that we can select a timestamped value that is not the most recent value.
    mWriter.put(mEntityId, "family", "qual0", 4L, "oldest");
    mWriter.put(mEntityId, "family", "qual0", 6L, "middle");
    mWriter.put(mEntityId, "family", "qual0", 8L, "newest");

    mWriter.put(mEntityId, "family", "qual1", 1L, "one");
    mWriter.put(mEntityId, "family", "qual1", 2L, "two");
    mWriter.put(mEntityId, "family", "qual1", 3L, "three");
    mWriter.put(mEntityId, "family", "qual1", 4L, "four");
    mWriter.put(mEntityId, "family", "qual1", 8L, "eight");

    mWriter.put(mEntityId, "family", "qual2", 3L, "q2-three");
    mWriter.put(mEntityId, "family", "qual2", 4L, "q2-four");
    mWriter.put(mEntityId, "family", "qual2", 6L, "q2-six");

    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .withTimeRange(2L, 7L)
        .addColumns(ColumnsDef.create().add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual2"))
        .build();

    final FijiRowData row1 = mReader.get(mEntityId, dataRequest);

    // This should be "middle" based on the time range of the data request.
    final String qual0val = row1.getMostRecentValue("family", "qual0").toString();
    assertEquals("Didn't get the middle value for family:qual0", "middle", qual0val);

    // We always optimize maxVersions=1 to actually return exactly 1 value, even of
    // we requested more versions of other columns.
    final NavigableMap<Long, CharSequence> q0vals = row1.getValues("family", "qual0");
    assertEquals("qual0 should only return one thing", 1, q0vals.size());
    assertEquals("Newest (only) value in q0 should be 'middle'.",
        "middle", q0vals.firstEntry().getValue().toString());

    // qual1 should see at least two versions, but no newer than 7L.
    final NavigableMap<Long, CharSequence> q1vals = row1.getValues("family", "qual1");
    assertEquals("qual1 getValues should have exactly two items", 2, q1vals.size());
    assertEquals("Newest value in q1 should be 'four'.",
        "four", q1vals.firstEntry().getValue().toString());

    // qual2 should see exactly three versions.
    final NavigableMap<Long, CharSequence> q2vals = row1.getValues("family", "qual2");
    assertEquals("qual2 getValues should have exactly three items", 3, q2vals.size());
    assertEquals("Newest value in q2 should be 'q2-six'.",
        "q2-six", q2vals.firstEntry().getValue().toString());
  }

  @Test
  public void testEmptyResult() throws IOException {
    // TODO: Test having results for a family, but not for a particular qualifier.
    // TODO: Test not having results for family or qualifier.
    mWriter.put(mEntityId, "family", "qual0", 1L, "string1");
    mWriter.put(mEntityId, "family", "qual0", 2L, "string2");

    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "qual1"))
        .build();

    final FijiRowData row1 = mReader.get(mEntityId, dataRequest);

    final NavigableMap<Long, CharSequence> values = row1.getValues("family", "qual1");
    assertTrue("getValues should return an empty map for empty rowdata.", values.isEmpty());

    final NavigableMap<Long, FijiCell<CharSequence>> cells = row1.getCells("family", "qual1");
    assertTrue("getCells should return an empty map for empty rowdata.", cells.isEmpty());

    final Iterator<FijiCell<CharSequence>> iterator =  row1.iterator("family", "qual1");
    assertFalse("iterator obtained on a column the rowdata has no data for should return false"
        + "when hasNext is called.",
        iterator.hasNext());

    final CharSequence value = row1.getMostRecentValue("family", "qual1");
    assertEquals("getMostRecentValue should return a null value from an empty rowdata.",
        null,
        value);

    final FijiCell<CharSequence> cell = row1.getMostRecentCell("family", "qual1");
    assertEquals("getMostRecentCell should return a null cell from empty rowdata.",
        null,
        cell);
  }

  // This test was created in response to WIBI-41.  If your FijiDataRequest doesn't contain
  // one of the columns in the Result map, you used to a get a NullPointerException.
  @Test
  public void testGetMap() throws Exception {
    // Put some data into the table.
    mWriter.put(mEntityId, FAMILY, QUAL0, "bot");
    mWriter.put(mEntityId, FAMILY, EMPTY, "car");

    final FijiDataRequest dataRequest = FijiDataRequest.builder().build();

    // We didn't request any data, so the map should be null.
    final FijiRowData input = mReader.get(mEntityId, dataRequest);
  }

  @Test
  public void testContainsColumn() throws Exception {
    final long myTime = 1L;
    mWriter.put(mEntityId, FAMILY, QUAL0, myTime, "foo");

    FijiRowData row1 = mReader.get(mEntityId, FijiDataRequest.create(FAMILY, QUAL0));
    assertTrue(row1.containsCell(FAMILY, QUAL0, myTime));
    assertFalse(row1.containsCell(FAMILY, QUAL0, myTime + 1L));
    assertFalse(row1.containsCell("fake", QUAL0, myTime));
    assertFalse(row1.containsCell(FAMILY, "fake", myTime));
  }

  @Test
  public void testIteratorMapFamilyTypes() throws IOException {
    mWriter.put(mEntityId, MAP, KEY0, 1L, 0);
    mWriter.put(mEntityId, MAP, KEY1, 1L, 1);
    mWriter.put(mEntityId, MAP, KEY2, 1L, 2);
    mWriter.put(mEntityId, FAMILY, QUAL0, 1L, "string1");
    mWriter.put(mEntityId, FAMILY, QUAL0, 2L, "string2");

    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily(MAP))
        .build();

    final FijiRowData row1 = mReader.get(mEntityId, dataRequest);
    final Iterator<FijiCell<Integer>> cells = row1.iterator(MAP);

    assertTrue(cells.hasNext());
    final FijiCell<?> cell0 = cells.next();
    assertEquals("Wrong first cell!", KEY0, cell0.getColumn().getQualifier());

    assertTrue(cells.hasNext());
    final FijiCell<?> cell1 = cells.next();
    assertEquals("Wrong second cell!", KEY1, cell1.getColumn().getQualifier());

    assertTrue(cells.hasNext());
    final FijiCell<?> cell2 = cells.next();
    assertEquals("Wrong third cell!", KEY2, cell2.getColumn().getQualifier());
    assertFalse(cells.hasNext());

    final Iterator<FijiCell<Integer>> cellsKey1 = row1.iterator("map", "key1");
    assertTrue(cellsKey1.hasNext());

    final FijiCell<Integer> key1Cell = cellsKey1.next();
    assertEquals("key1", key1Cell.getColumn().getQualifier());
    assertEquals(1L, key1Cell.getTimestamp());
    assertEquals((Integer) 1, key1Cell.getData());
    assertFalse(cellsKey1.hasNext());
  }

  @Test
  public void testIteratorMapFamilyMaxVersionsTypes() throws IOException {
    mWriter.put(mEntityId, MAP, KEY0, 1L, 0);
    mWriter.put(mEntityId, MAP, KEY0, 2L, 1);
    mWriter.put(mEntityId, MAP, KEY0, 3L, 2);

    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).addFamily("map"))
        .build();

    final FijiRowData row1 = mReader.get(mEntityId, dataRequest);
    final Iterator<FijiCell<Integer>> cells = row1.iterator(MAP);
    assertTrue(cells.hasNext());

    final FijiCell<Integer> cell0 = cells.next();
    assertEquals("Wrong first cell!", 2, cell0.getData().intValue());
    assertTrue(cells.hasNext());

    final FijiCell<Integer> cell1 = cells.next();
    assertEquals("Wrong second cell!", 1, cell1.getData().intValue());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testMapAsIterable() throws IOException {
    mWriter.put(mEntityId, MAP, KEY0, 1L, 0);
    mWriter.put(mEntityId, MAP, KEY1, 1L, 1);
    mWriter.put(mEntityId, MAP, KEY2, 1L, 2);
    mWriter.put(mEntityId, FAMILY, QUAL0, 1L, "string1");
    mWriter.put(mEntityId, FAMILY, QUAL0, 2L, "string2");

    final FijiDataRequest dataRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).addFamily("map"))
        .build();

    final FijiRowData row1 = mReader.get(mEntityId, dataRequest);
    final List<FijiCell<Integer>> cells = Lists.newArrayList(row1.<Integer>asIterable(MAP));
    final int cellCount = cells.size();
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }
}
