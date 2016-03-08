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

import com.google.common.base.Objects;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;

/**
 * Translates between HBase and Fiji column names.
 *
 * <p>This class defines a mapping between names of Fiji table locality group/family/qualifiers and
 * HBase families/qualifiers. The identity mapping uses the Fiji locality group as the HBase
 * column family and the Fiji family:qualifier as the HBase qualifier.</p>
 *
 * <p>This class is not for the purpose of reading native HBase tables whose HBase
 * family/qualifiers are not in the localityGroup:family:qualifier format.</p>
 */
@ApiAudience.Private
public final class IdentityColumnNameTranslator extends HBaseColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(IdentityColumnNameTranslator.class);

  /** Used to separate the Fiji family from the Fiji qualifier in an HBase qualifier. */
  private static final byte SEPARATOR = Bytes.toBytes(":")[0];

  /** The table to translate names for. */
  private final FijiTableLayout mLayout;

  /**
   * Creates a new {@link IdentityColumnNameTranslator} instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public IdentityColumnNameTranslator(FijiTableLayout tableLayout) {
    mLayout = tableLayout;
  }

  /** {@inheritDoc}*/
  @Override
  public FijiColumnName toFijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException {
    LOG.debug("Translating HBase column name {} to Fiji column name.", hbaseColumnName);

    final String localityGroupName = Bytes.toString(hbaseColumnName.getFamily());

    final LocalityGroupLayout localityGroup = mLayout.getLocalityGroupMap().get(localityGroupName);
    if (localityGroup == null) {
      throw new NoSuchColumnException(String.format("No locality group %s in table %s.",
          localityGroupName, mLayout.getName()));
    }

    // Parse the HBase qualifier as a byte[] in order to save a String instantiation
    final byte[] hbaseQualifier = hbaseColumnName.getQualifier();
    final int index = ArrayUtils.indexOf(hbaseQualifier, SEPARATOR);
    if (index == -1) {
      throw new NoSuchColumnException(String.format(
          "Missing separator in HBase column %s.", hbaseColumnName));
    }
    final String familyName = Bytes.toString(hbaseQualifier, 0, index);
    final String qualifierName =
        Bytes.toString(hbaseQualifier, index + 1, hbaseQualifier.length - index - 1);

    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No family %s in locality group %s of table %s.",
          familyName, localityGroupName, mLayout.getName()));
    }

    if (family.isGroupType()) {
      // Group type family.
      if (!family.getColumnMap().containsKey(qualifierName)) {
        throw new NoSuchColumnException(String.format(
            "No qualifier %s in family %s of table %s.",
            qualifierName, familyName, mLayout.getName()));
      }
      final FijiColumnName fijiColumnName = new FijiColumnName(familyName, qualifierName);
      LOG.debug("Translated to Fiji group type column {}.", fijiColumnName);
      return fijiColumnName;
    } else {
      // Map type family.
      assert family.isMapType();
      final FijiColumnName fijiColumnName = new FijiColumnName(familyName, qualifierName);
      LOG.debug("Translated to Fiji map type column '{}'.", fijiColumnName);
      return fijiColumnName;
    }
  }

  /** {@inheritDoc}*/
  @Override
  public HBaseColumnName toHBaseColumnName(FijiColumnName fijiColumnName)
      throws NoSuchColumnException {

    final String familyName = fijiColumnName.getFamily();
    final String qualifierName = fijiColumnName.getQualifier();

    // Validate the Fiji family
    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(fijiColumnName.toString());
    }

    // Validate the Fiji qualifier
    if (family.isGroupType() && !family.getColumnMap().containsKey(qualifierName)) {
      throw new NoSuchColumnException(fijiColumnName.toString());
    }

    final byte[] localityGroupBytes = Bytes.toBytes(family.getLocalityGroup().getName());
    final byte[] familyBytes = Bytes.toBytes(familyName);
    final byte[] qualifierBytes = Bytes.toBytes(qualifierName);

    final byte[] hbaseQualifierBytes =
        ShortColumnNameTranslator.concatWithSeparator(SEPARATOR, familyBytes, qualifierBytes);

    return new HBaseColumnName(localityGroupBytes, hbaseQualifierBytes);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] toHBaseFamilyName(LocalityGroupLayout localityGroup) {
    return Bytes.toBytes(localityGroup.getName());
  }

  /** {@inheritDoc}*/
  @Override
  public FijiTableLayout getTableLayout() {
    return mLayout;
  }

  /** {@inheritDoc}*/
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", mLayout.getName())
        .add("layout", mLayout)
        .toString();
  }
}
