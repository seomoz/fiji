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
package com.moz.fiji.schema.impl.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiTableAnnotator;
import com.moz.fiji.schema.cassandra.CassandraFijiClientTest;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestCassandraFijiTableAnnotator extends CassandraFijiClientTest {

  private static final String KEY = "abc";
  private static final String VALUE = "def";
  private static final String KEY2 = "123";
  private static final String VALUE2 = "456";
  private static final String KEY3 = "zyx";
  private static final String VALUE3 = "wvu";
  private static final Map<String, String> KVS =
      ImmutableMap.<String, String>builder().put(KEY, VALUE).put(KEY2, VALUE2).build();
  private static final Set<String> KEYS = Sets.newHashSet(KEY, KEY2);
  private static final FijiColumnName INFONAME = new FijiColumnName("info:name");
  private static final FijiColumnName INFOEMAIL = new FijiColumnName("info:email");
  private static final String INFO = "info";

  private Fiji mFiji = null;
  private CassandraFijiTable mTable = null;
  private FijiTableAnnotator mAnnotator = null;

  @Before
  public void setup() throws IOException {
    mFiji = new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.USER_TABLE_FORMATTED_EID))
        .build();
    mTable = (CassandraFijiTable) mFiji.openTable("user");
    mAnnotator = mTable.openTableAnnotator();
  }

  @After
  public void cleanup() throws IOException {
    mAnnotator.close();
    mTable.release();
  }

  @Test
  public void testRegex() {
    final String valid = "abcABC012_";
    assertTrue(CassandraFijiTableAnnotator.isValidAnnotationKey(valid));

    final List<String> invalidStrings =
        Lists.newArrayList("abc?", "abc.", "a$", "a!", "a#", "a-", "");
    for (String invalid : invalidStrings) {
      assertFalse(CassandraFijiTableAnnotator.isValidAnnotationKey(invalid));
    }
  }

  @Test
  public void testLoop() throws IOException {
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertEquals(VALUE, mAnnotator.getColumnAnnotation(INFONAME, KEY));
    mAnnotator.setColumnAnnotation(INFONAME, KEY2, VALUE2);
    assertEquals(VALUE2, mAnnotator.getColumnAnnotation(INFONAME, KEY2));

    final Map<String, String> annotations = mAnnotator.getAllColumnAnnotations(INFONAME);
    assertEquals(2, annotations.size());
    assertTrue(annotations.containsKey(KEY) && annotations.containsKey(KEY2));

    mAnnotator.removeAllColumnAnnotations(INFONAME);
    assertTrue(mAnnotator.getAllColumnAnnotations(INFONAME).isEmpty());

    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY2, VALUE2);
    final Map<FijiColumnName, Map<String, String>> allAnnotations =
        mAnnotator.getAllColumnAnnotations();
    assertEquals(2, allAnnotations.size());
    assertEquals(VALUE, allAnnotations.get(INFONAME).get(KEY));
    assertEquals(VALUE2, allAnnotations.get(INFOEMAIL).get(KEY2));
  }

  @Test
  public void testSet() throws IOException {
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    mAnnotator.setColumnAnnotations(INFOEMAIL, KVS);
    mAnnotator.setTableAnnotation(KEY3, VALUE3);
    mAnnotator.setTableAnnotations(KVS);

    assertEquals(VALUE, Bytes.toString(mFiji.getMetaTable().getValue(mTable.getName(),
        CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY))));
    assertEquals(VALUE, Bytes.toString(mFiji.getMetaTable().getValue(mTable.getName(),
        CassandraFijiTableAnnotator.getMetaTableKey(
            mTable, INFOEMAIL, KEY))));
    assertEquals(VALUE2, Bytes.toString(mFiji.getMetaTable().getValue(mTable.getName(),
        CassandraFijiTableAnnotator.getMetaTableKey(
            mTable, INFOEMAIL, KEY2))));
    assertEquals(VALUE, Bytes.toString(mFiji.getMetaTable().getValue(mTable.getName(),
        CassandraFijiTableAnnotator.getMetaTableKey(KEY))));
    assertEquals(VALUE2, Bytes.toString(mFiji.getMetaTable().getValue(mTable.getName(),
        CassandraFijiTableAnnotator.getMetaTableKey(KEY2))));
    assertEquals(VALUE3, Bytes.toString(mFiji.getMetaTable().getValue(mTable.getName(),
        CassandraFijiTableAnnotator.getMetaTableKey(KEY3))));
  }

  // CSOFF: MethodLengthCheck
  @Test
  public void testRemove() throws IOException {
    mAnnotator.setTableAnnotation(KEY, VALUE);
    mAnnotator.removeTableAnnotation(KEY);
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY)), ioe.getMessage());
    }

    mAnnotator.setTableAnnotation(KEY2, VALUE2);
    assertEquals(
        Sets.newHashSet(KEY2), mAnnotator.removeTableAnnotationsStartingWith(KEY2.substring(0, 1)));
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY2));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY2)), ioe.getMessage());
    }

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertEquals(
        Sets.newHashSet(KEY), mAnnotator.removeTableAnnotationsContaining(KEY.substring(1, 2)));
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY)), ioe.getMessage());
    }

    mAnnotator.setTableAnnotation(KEY2, VALUE2);
    assertEquals(Sets.newHashSet(KEY2), mAnnotator.removeTableAnnotationsMatching("^[0-9]*$"));
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY2));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY2)), ioe.getMessage());
    }

    mAnnotator.setTableAnnotations(KVS);
    assertEquals(KEYS, mAnnotator.removeAllTableAnnotations());
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY2));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format(
          "Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(KEY2)), ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    mAnnotator.removeColumnAnnotation(INFONAME, KEY);
    try {
      mFiji.getMetaTable().getValue(mTable.getName(),
          CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY)), ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFONAME, KEY2, VALUE2);
    assertEquals(Sets.newHashSet(KEY2),
        mAnnotator.removeColumnAnnotationsStartingWith(INFONAME, KEY2.substring(0, 1)));
    try {
      mFiji.getMetaTable().getValue(mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY2));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY2)), ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    assertEquals(Sets.newHashSet(KEY),
        mAnnotator.removeColumnAnnotationsContaining(INFOEMAIL, KEY.substring(1, 2)));
    try {
      mFiji.getMetaTable().getValue(mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFOEMAIL, KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFOEMAIL, KEY)), ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY2, VALUE2);
    assertEquals(
        Sets.newHashSet(KEY2), mAnnotator.removeColumnAnnotationsMatching(INFOEMAIL, "^[0-9]*$"));
    try {
      mFiji.getMetaTable().getValue(mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFOEMAIL, KEY2));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFOEMAIL, KEY2)), ioe.getMessage());
    }

    mAnnotator.setColumnAnnotations(INFONAME, KVS);
    assertEquals(KEYS, mAnnotator.removeAllColumnAnnotations(INFONAME));
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY2));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY2)), ioe.getMessage());
    }


    final Map<FijiColumnName, Set<String>> removedMatcher = Maps.newHashMap();
    removedMatcher.put(INFONAME, Sets.newHashSet(KEY));

    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertEquals(Sets.newHashSet(INFONAME), mAnnotator.removeColumnAnnotationsInFamily(INFO, KEY));
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format(
          "Could not find any values associated with table %s and key %s",
          mTable.getName(),
          CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY)),
          ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertEquals(removedMatcher,
        mAnnotator.removeColumnAnnotationsInFamilyStartingWith(INFO, KEY.substring(0, 1)));
    try {
      mFiji.getMetaTable().getValue(
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format("Could not find any values associated with table %s and key %s",
          mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY)), ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertEquals(removedMatcher,
        mAnnotator.removeColumnAnnotationsInFamilyContaining(INFO, KEY.substring(1, 2)));
    try {
      mFiji.getMetaTable().getValue(mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format(
          "Could not find any values associated with table %s and key %s",
          mTable.getName(),
          CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY)),
          ioe.getMessage());
    }

    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertEquals(removedMatcher,
        mAnnotator.removeColumnAnnotationsInFamilyMatching(INFO, "^[a-z]*$"));
    try {
      mFiji.getMetaTable().getValue(mTable.getName(), CassandraFijiTableAnnotator.getMetaTableKey(
          mTable, INFONAME, KEY));
      fail("Should have thrown IOException for missing key.");
    } catch (IOException ioe) {
      assertEquals(String.format(
          "Could not find any values associated with table %s and key %s",
          mTable.getName(),
          CassandraFijiTableAnnotator.getMetaTableKey(mTable, INFONAME, KEY)),
          ioe.getMessage());
    }
  }
  // CSON: MethodLengthCheck

  @Test
  public void testGet() throws IOException {

    // Table

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertEquals(VALUE, mAnnotator.getTableAnnotation(KEY));

    final Map<String, String> tableAll = mAnnotator.getAllTableAnnotations();
    assertEquals(1, tableAll.size());
    assertEquals(VALUE, tableAll.get(KEY));

    final Map<String, String> tableStarting =
        mAnnotator.getTableAnnotationsStartingWith(KEY.substring(0, 1));
    assertEquals(1, tableStarting.size());
    assertEquals(VALUE, tableStarting.get(KEY));

    final Map<String, String> tableContaining =
        mAnnotator.getTableAnnotationsContaining(KEY.substring(1, 2));
    assertEquals(1, tableContaining.size());
    assertEquals(VALUE, tableContaining.get(KEY));

    final Map<String, String> tableMatching = mAnnotator.getTableAnnotationsMatching("^[a-z]*$");
    assertEquals(1, tableMatching.size());
    assertEquals(VALUE, tableMatching.get(KEY));

    // Column

    mAnnotator.setColumnAnnotation(INFONAME, KEY2, VALUE2);
    assertEquals(VALUE2, mAnnotator.getColumnAnnotation(INFONAME, KEY2));

    final Map<String, String> columnAll = mAnnotator.getAllColumnAnnotations(INFONAME);
    assertEquals(1, columnAll.size());
    assertEquals(VALUE2, columnAll.get(KEY2));

    final Map<String, String> columnStarting =
        mAnnotator.getColumnAnnotationsStartingWith(INFONAME, KEY2.substring(0, 1));
    assertEquals(1, columnStarting.size());
    assertEquals(VALUE2, columnStarting.get(KEY2));

    final Map<String, String> columnContaining =
        mAnnotator.getColumnAnnotationsContaining(INFONAME, KEY2.substring(1, 2));
    assertEquals(1, columnContaining.size());
    assertEquals(VALUE2, columnContaining.get(KEY2));

    final Map<String, String> columnMatching =
        mAnnotator.getColumnAnnotationsMatching(INFONAME, "^[0-9]*$");
    assertEquals(1, columnMatching.size());
    assertEquals(VALUE2, columnMatching.get(KEY2));

    // Family

    mAnnotator.setColumnAnnotation(INFONAME, KEY3, VALUE3);
    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY3, VALUE3);
    final Map<FijiColumnName, String> exact = mAnnotator.getColumnAnnotationsInFamily(INFO, KEY3);
    assertEquals(2, exact.size());
    assertEquals(VALUE3, exact.get(INFONAME));
    assertEquals(VALUE3, exact.get(INFOEMAIL));

    final Map<FijiColumnName, Map<String, String>> familyAll =
        mAnnotator.getAllColumnAnnotationsInFamily(INFO);
    assertEquals(2, familyAll.size());
    assertEquals(VALUE3, familyAll.get(INFONAME).get(KEY3));
    assertEquals(VALUE3, familyAll.get(INFOEMAIL).get(KEY3));

    final Map<FijiColumnName, Map<String, String>> familyStarting =
        mAnnotator.getColumnAnnotationsInFamilyStartingWith(INFO, KEY3.substring(0, 1));
    assertEquals(2, familyStarting.size());
    assertEquals(VALUE3, familyStarting.get(INFONAME).get(KEY3));
    assertEquals(VALUE3, familyStarting.get(INFOEMAIL).get(KEY3));

    final Map<FijiColumnName, Map<String, String>> familyContaining =
        mAnnotator.getColumnAnnotationsInFamilyContaining(INFO, KEY3.substring(1, 2));
    assertEquals(2, familyContaining.size());
    assertEquals(VALUE3, familyContaining.get(INFONAME).get(KEY3));
    assertEquals(VALUE3, familyContaining.get(INFOEMAIL).get(KEY3));

    final Map<FijiColumnName, Map<String, String>> familyMatching =
        mAnnotator.getColumnAnnotationsInFamilyMatching(INFO, "^[a-z]*$");
    assertEquals(2, familyMatching.size());
    assertEquals(VALUE3, familyMatching.get(INFONAME).get(KEY3));
    assertEquals(VALUE3, familyMatching.get(INFOEMAIL).get(KEY3));
  }
}
