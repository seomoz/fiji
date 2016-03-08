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

public class TestHBaseNativeColumnNameTranslator {
  private HBaseColumnNameTranslator mTranslator;

  @Before
  public void readLayout() throws Exception {
    final FijiTableLayout tableLayout =
        FijiTableLayout.newLayout(
            FijiTableLayouts.getLayout(
                FijiTableLayouts.FULL_FEATURED_NATIVE));

    mTranslator = HBaseColumnNameTranslator.from(tableLayout);
  }

  @Test
  public void testTranslateFromFijiToHBase() throws Exception {
    HBaseColumnName infoName = mTranslator.toHBaseColumnName(FijiColumnName.create("info:name"));
    assertEquals("info", infoName.getFamilyAsString());
    assertEquals("name", infoName.getQualifierAsString());

    HBaseColumnName infoEmail = mTranslator.toHBaseColumnName(FijiColumnName.create("info:email"));
    assertEquals("info", infoEmail.getFamilyAsString());
    assertEquals("email", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = mTranslator.toHBaseColumnName(
        FijiColumnName.create("recommendations:product"));
    assertEquals("recommendations", recommendationsProduct.getFamilyAsString());
    assertEquals("product", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases = mTranslator.toHBaseColumnName(
        FijiColumnName.create("recommendations:product"));
    assertEquals("recommendations", purchases.getFamilyAsString());
    assertEquals("product", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToFiji() throws Exception {
    FijiColumnName infoName =
        mTranslator.toFijiColumnName(getHBaseColumnName("info", "name"));
    assertEquals("info:name", infoName.toString());

    FijiColumnName infoEmail =
        mTranslator.toFijiColumnName(getHBaseColumnName("info", "email"));
    assertEquals("info:email", infoEmail.toString());

    FijiColumnName recommendationsProduct = mTranslator.toFijiColumnName(
        getHBaseColumnName("recommendations", "product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    FijiColumnName purchases =
        mTranslator.toFijiColumnName(getHBaseColumnName("recommendations", "product"));
    assertEquals("recommendations:product", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Fiji locality group.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchFijiLocalityGroup() throws Exception {
    mTranslator.toFijiColumnName(getHBaseColumnName("fakeFamily", "fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Fiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchFijiColumn() throws Exception {
    mTranslator.toFijiColumnName(getHBaseColumnName("recommendations", "fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Fiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchHBaseColumn() throws Exception {
    mTranslator.toHBaseColumnName(FijiColumnName.create("doesnt:exist"));
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a NativeFijiColumnTranslator
   * when the layout contains column families that don't match the locality group.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNativeLayout() throws Exception {
    FijiTableLayout invalidLayout =
        FijiTableLayout.newLayout(FijiTableLayouts.getLayout(FijiTableLayouts.INVALID_NATIVE));
    HBaseColumnNameTranslator.from(invalidLayout);
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
