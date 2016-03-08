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

package com.moz.fiji.schema.layout.impl.cassandra;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.cassandra.CassandraColumnName;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import com.moz.fiji.schema.layout.impl.ColumnId;

/**
 * Translates {@link FijiColumnName}s into {@link CassandraColumnName}s.
 */
@ApiAudience.Private
public final class ShortColumnNameTranslator extends CassandraColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(ShortColumnNameTranslator.class);

  private final FijiTableLayout mLayout;

  /**
   * Creates a new {@link com.moz.fiji.schema.layout.impl.cassandra.ShortColumnNameTranslator} instance.
   *
   * @param layout of the table to translate column names for.
   */
  public ShortColumnNameTranslator(final FijiTableLayout layout) {
    mLayout = layout;
  }

  /** {@inheritDoc} */
  @Override
  public FijiColumnName toFijiColumnName(
      final CassandraTableName localityGroupTable,
      final CassandraColumnName cassandraColumnName
  ) throws NoSuchColumnException {

    final String localityGroupName =
        mLayout.getLocalityGroupIdNameMap().get(localityGroupTable.getLocalityGroupId());
    final LocalityGroupLayout localityGroup = mLayout.getLocalityGroupMap().get(localityGroupName);
        mLayout.getLocalityGroupIdNameMap().get(localityGroupTable.getLocalityGroupId());
    if (localityGroup == null) {
      throw new NoSuchColumnException(
          String.format("No locality group for Cassandra table %s in Fiji table %s.",
              localityGroupTable, mLayout.getName()));
    }

    final ColumnId familyID = ColumnId.fromByteArray(cassandraColumnName.getFamily());
    final FamilyLayout family =
        localityGroup.getFamilyMap().get(localityGroup.getFamilyIdNameMap().get(familyID));
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No family with ID %s in locality group %s of table %s.",
          familyID.getId(), localityGroup.getName(), mLayout.getName()));
    }

    final FijiColumnName fijiColumnName;
    if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = ColumnId.fromByteArray(cassandraColumnName.getQualifier());
      final ColumnLayout qualifier =
          family.getColumnMap().get(family.getColumnIdNameMap().get(qualifierID));
      if (qualifier == null) {
        throw new NoSuchColumnException(String.format(
            "No column with ID %s in family %s of table %s.",
            qualifierID.getId(), family.getName(), mLayout.getName()));
      }
      fijiColumnName = FijiColumnName.create(family.getName(), qualifier.getName());
    } else {
      // Map type family.
      assert(family.isMapType());
      fijiColumnName =
          FijiColumnName.create(
              family.getName(),
              Bytes.toString(cassandraColumnName.getQualifier()));
    }
    LOG.debug("Translated Fiji column {}.", fijiColumnName);
    return fijiColumnName;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraColumnName toCassandraColumnName(
      final FijiColumnName fijiColumnName
  ) throws NoSuchColumnException {
    final String familyName = fijiColumnName.getFamily();
    final String qualifierName = fijiColumnName.getQualifier();

    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(fijiColumnName.toString());
    }

    final ColumnId familyID = family.getId();

    final byte[] familyBytes = familyID.toByteArray();

    if (qualifierName == null) {
      // Unqualified column
      return new CassandraColumnName(familyBytes, null);
    } else if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = family.getColumnIdNameMap().inverse().get(qualifierName);
      final byte[] qualifierBytes = qualifierID.toByteArray();
      return new CassandraColumnName(familyBytes, qualifierBytes);
    } else {
      // Map type family.
      assert family.isMapType();
      final byte[] qualifierBytes = Bytes.toBytes(qualifierName);
      return new CassandraColumnName(familyBytes, qualifierBytes);
    }
  }
}
