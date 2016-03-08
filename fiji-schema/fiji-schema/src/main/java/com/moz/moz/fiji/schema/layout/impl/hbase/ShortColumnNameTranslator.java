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
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import com.moz.fiji.schema.layout.impl.ColumnId;

/**
 * Translates between HBase and Fiji column names.
 *
 * <p>This class defines a mapping between names of HBase families/qualifiers and
 * Fiji locality group/family/qualifiers using a custom base-64 encoding based on the ids of each
 * individual column.</p>
 */
@ApiAudience.Private
public final class ShortColumnNameTranslator extends HBaseColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(ShortColumnNameTranslator.class);

  /** Used to separate the Fiji family from the Fiji qualifier in an HBase qualifier. */
  private static final byte SEPARATOR = ':';

  /** The table to translate names for. */
  private final FijiTableLayout mLayout;

  /**
   * Creates a new {@link ShortColumnNameTranslator} instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public ShortColumnNameTranslator(FijiTableLayout tableLayout) {
    mLayout = tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public FijiColumnName toFijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException {
    LOG.debug("Translating HBase column name '{}' to Fiji column name...", hbaseColumnName);
    final ColumnId localityGroupID = ColumnId.fromByteArray(hbaseColumnName.getFamily());
    final LocalityGroupLayout localityGroup =
        mLayout.getLocalityGroupMap().get(mLayout.getLocalityGroupIdNameMap().get(localityGroupID));
    if (localityGroup == null) {
      throw new NoSuchColumnException(String.format("No locality group with ID %s in table %s.",
          localityGroupID.getId(), mLayout.getName()));
    }

    // Parse the HBase qualifier as a byte[] in order to save a String instantiation
    final byte[] hbaseQualifier = hbaseColumnName.getQualifier();
    final int index = ArrayUtils.indexOf(hbaseQualifier, SEPARATOR);
    if (index == -1) {
      throw new NoSuchColumnException(String.format(
          "Missing separator in HBase column %s.", hbaseColumnName));
    }
    final ColumnId familyID = ColumnId.fromString(Bytes.toString(hbaseQualifier, 0, index));
    final String rawQualifier =
        Bytes.toString(hbaseQualifier, index + 1, hbaseQualifier.length - index - 1);

    final FamilyLayout family =
        localityGroup.getFamilyMap().get(localityGroup.getFamilyIdNameMap().get(familyID));
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No family with ID %s in locality group %s of table %s.",
          familyID.getId(), localityGroup.getName(), mLayout.getName()));
    }

    if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = ColumnId.fromString(rawQualifier);
      final ColumnLayout qualifier =
          family.getColumnMap().get(family.getColumnIdNameMap().get(qualifierID));
      if (qualifier == null) {
        throw new NoSuchColumnException(String.format(
            "No column with ID %s in family %s of table %s.",
            qualifierID.getId(), family.getName(), mLayout.getName()));
      }
      final FijiColumnName fijiColumnName =
          new FijiColumnName(family.getName(), qualifier.getName());
      LOG.debug("Translated to Fiji group column {}.", fijiColumnName);
      return fijiColumnName;
    } else {
      // Map type family.
      assert family.isMapType();
      final FijiColumnName fijiColumnName = new FijiColumnName(family.getName(), rawQualifier);
      LOG.debug("Translated to Fiji map column '{}'.", fijiColumnName);
      return fijiColumnName;
    }
  }

  /** {@inheritDoc} */
  @Override
  public HBaseColumnName toHBaseColumnName(final FijiColumnName fijiColumnName)
      throws NoSuchColumnException {
    final String familyName = fijiColumnName.getFamily();
    final String qualifierName = fijiColumnName.getQualifier();

    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(fijiColumnName.toString());
    }

    final ColumnId localityGroupID = family.getLocalityGroup().getId();
    final ColumnId familyID = family.getId();

    final byte[] localityGroupBytes = Bytes.toBytes(localityGroupID.toString());
    final byte[] familyBytes = Bytes.toBytes(familyID.toString());

    if (qualifierName == null) {
      // Unqualified column
      return new HBaseColumnName(localityGroupBytes,
          concatWithSeparator(SEPARATOR, familyBytes, new byte[]{}));
    } else if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = family.getColumnIdNameMap().inverse().get(qualifierName);
      final byte[] qualifierBytes = Bytes.toBytes(qualifierID.toString());

      return new HBaseColumnName(localityGroupBytes,
          concatWithSeparator(SEPARATOR, familyBytes, qualifierBytes));
    } else {
      // Map type family.
      assert family.isMapType();
      final byte[] qualifierBytes = Bytes.toBytes(qualifierName);

      return new HBaseColumnName(
          localityGroupBytes,
          concatWithSeparator(SEPARATOR, familyBytes, qualifierBytes));
    }
  }

  /** {@inheritDoc} */
  @Override
  public byte[] toHBaseFamilyName(LocalityGroupLayout localityGroup) {
    return Bytes.toBytes(localityGroup.getId().toString());
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableLayout getTableLayout() {
    return mLayout;
  }

  /**
   * Append the byte arrays with a single byte separator.
   *
   * @param separator to insert between the two arrays.
   * @param a first byte array.
   * @param b second byte array.
   * @return a concatenated with b with a separator byte in between.
   */
  static byte[] concatWithSeparator(byte separator, byte[] a, byte[] b) {
    final byte[] ret = new byte[a.length + b.length + 1];
    System.arraycopy(
        a, 0,      // src, src position
        ret, 0,    // dst, dst position
        a.length); // length
    ret[a.length] = separator;
    System.arraycopy(
        b, 0,              // src, src position
        ret, a.length + 1, // dst, dst position
        b.length);         // length
    return ret;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", mLayout.getName())
        .add("layout", mLayout)
        .toString();
  }
}
