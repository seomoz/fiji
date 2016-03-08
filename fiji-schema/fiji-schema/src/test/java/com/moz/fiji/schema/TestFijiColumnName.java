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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestFijiColumnName {
  @Test
  public void testNull() {
    try {
      FijiColumnName.create(null);
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Column name may not be null. At least specify family", iae.getMessage());
    }
  }

  @Test
  public void testNullFamily() {
    try {
      FijiColumnName.create(null, "qualifier");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Family name may not be null.", iae.getMessage());
    }
  }

  @Test
  public void testMapFamily() {
    FijiColumnName columnName = FijiColumnName.create("family");
    assertEquals("family", columnName.getFamily());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifier());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testEmptyQualifier() {
    FijiColumnName columnName = FijiColumnName.create("family:");
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testEmptyQualifierTwo() {
    FijiColumnName columnName = FijiColumnName.create("family", "");
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testNormal() {
    FijiColumnName columnName = FijiColumnName.create("family:qualifier");
    assertEquals("family", columnName.getFamily());
    assertEquals("qualifier", columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertArrayEquals(Bytes.toBytes("qualifier"), columnName.getQualifierBytes());
    assertTrue(columnName.isFullyQualified());
  }

  @Test
  public void testNullQualifier() {
    FijiColumnName columnName = FijiColumnName.create("family", null);
    assertEquals("family", columnName.getFamily());
    assertNull(columnName.getQualifier());
    assertArrayEquals(Bytes.toBytes("family"), columnName.getFamilyBytes());
    assertNull(columnName.getQualifierBytes());
    assertFalse(columnName.isFullyQualified());
  }

  @Test
  public void testInvalidFamilyName() {
    try {
      FijiColumnName.create("1:qualifier");
      fail("An exception should have been thrown.");
    } catch (FijiInvalidNameException kine) {
      assertEquals("Invalid family name: 1 Name must match pattern: [a-zA-Z_][a-zA-Z0-9_]*",
          kine.getMessage());
    }
  }

  @Test
  public void testEquals() {
    FijiColumnName columnA = FijiColumnName.create("family", "qualifier1");
    FijiColumnName columnC = FijiColumnName.create("family", null);
    FijiColumnName columnD = FijiColumnName.create("family", "qualifier2");
    assertTrue(columnA.equals(columnA)); // reflexive
    assertFalse(columnA.equals(columnC) || columnC.equals(columnA));
    assertFalse(columnA.equals(columnD) || columnD.equals(columnA));
  }

  @Test
  public void testHashCode() {
    FijiColumnName columnA = FijiColumnName.create("family", "qualifier");
    FijiColumnName columnB = FijiColumnName.create("family:qualifier");
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  public void testCompareTo() {
    FijiColumnName columnA = FijiColumnName.create("family");
    FijiColumnName columnB = FijiColumnName.create("familyTwo");
    FijiColumnName columnC = FijiColumnName.create("family:qualifier");
    assertTrue(0 == columnA.compareTo(columnA));
    assertTrue(0 > columnA.compareTo(columnB) && 0 < columnB.compareTo(columnA));
    assertTrue(0 > columnA.compareTo(columnC) && 0 < columnC.compareTo(columnA));
    assertTrue(0 < columnB.compareTo(columnC) && 0 > columnC.compareTo(columnB));
  }
}
