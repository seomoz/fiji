/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.schema.layout.impl.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestIdentityColumnNameTranslator {
  private HBaseColumnNameTranslator mTranslator;

  @Before
  public void readLayout() throws Exception {
    final FijiTableLayout tableLayout =
        FijiTableLayout.newLayout(
            FijiTableLayouts.getLayout(
                FijiTableLayouts.FULL_FEATURED_IDENTITY));

    mTranslator = HBaseColumnNameTranslator.from(tableLayout);
  }

  @Test
  public void testTranslateFromFijiToHBase() throws Exception {
    HBaseColumnName infoName = mTranslator.toHBaseColumnName(FijiColumnName.create("info:name"));
    assertEquals("default", infoName.getFamilyAsString());
    assertEquals("info:name", infoName.getQualifierAsString());

    HBaseColumnName infoEmail =
        mTranslator.toHBaseColumnName(FijiColumnName.create("info:email"));
    assertEquals("default", infoEmail.getFamilyAsString());
    assertEquals("info:email", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = mTranslator.toHBaseColumnName(
        FijiColumnName.create("recommendations:product"));
    assertEquals("inMemory", recommendationsProduct.getFamilyAsString());
    assertEquals("recommendations:product", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases =
        mTranslator.toHBaseColumnName(FijiColumnName.create("purchases:foo"));
    assertEquals("inMemory", purchases.getFamilyAsString());
    assertEquals("purchases:foo", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToFiji() throws Exception {
    FijiColumnName infoName =
        mTranslator.toFijiColumnName(getHBaseColumnName("default", "info:name"));
    assertEquals("info:name", infoName.toString());

    FijiColumnName infoEmail =
        mTranslator.toFijiColumnName(getHBaseColumnName("default", "info:email"));
    assertEquals("info:email", infoEmail.toString());

    FijiColumnName recommendationsProduct = mTranslator.toFijiColumnName(
        getHBaseColumnName("inMemory", "recommendations:product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    FijiColumnName purchases =
        mTranslator.toFijiColumnName(getHBaseColumnName("inMemory", "purchases:foo"));
    assertEquals("purchases:foo", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Fiji locality group.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchFijiLocalityGroup() throws Exception {
    mTranslator
        .toFijiColumnName(getHBaseColumnName("fakeLocalityGroup", "fakeFamily:fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the first part of the HBase qualifier doesn't
   * match a Fiji family.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchFijiFamily() throws Exception {
    mTranslator.toFijiColumnName(getHBaseColumnName("inMemory", "fakeFamily:fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Fiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchFijiColumn() throws Exception {
    mTranslator.toFijiColumnName(getHBaseColumnName("inMemory", "recommendations:fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the HBase qualifier is corrupt (no separator).
   */
  @Test(expected = NoSuchColumnException.class)
  public void testCorruptQualifier() throws Exception {
    mTranslator.toFijiColumnName(getHBaseColumnName("inMemory", "fakeFamilyfakeQualifier"));
  }

  /**
   * Tests translation of HBase qualifiers have multiple separators (the Fiji qualifier contains
   * the separator).
   */
  @Test
  public void testMultipleSeparators() throws Exception {
    FijiColumnName fijiColumnName =
        mTranslator.toFijiColumnName(getHBaseColumnName("inMemory", "purchases:left:right"));
    assertEquals("purchases", fijiColumnName.getFamily());
    assertEquals("left:right", fijiColumnName.getQualifier());
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Fiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchHBaseColumn() throws Exception {
    mTranslator.toHBaseColumnName(FijiColumnName.create("doesnt:exist"));
  }

  /**
   * Turns a family:qualifier string into an HBaseColumnName.
   *
   * @param family The HBase family.
   * @param qualifier the HBase qualifier.
   * @return An HBaseColumnName instance.
   */
  private static HBaseColumnName getHBaseColumnName(String family, String qualifier) {
    return new HBaseColumnName(Bytes.toBytes(family), Bytes.toBytes(qualifier));
  }
}
