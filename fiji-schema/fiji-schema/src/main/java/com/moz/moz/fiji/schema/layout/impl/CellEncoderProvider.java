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
package com.moz.fiji.schema.layout.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.FijiCellEncoder;
import com.moz.fiji.schema.FijiCellEncoderFactory;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Provider for cell encoders of a given table.
 *
 * <p>
 *   Cell encoders for all columns in the table are pro-actively created when the
 *   CellEncoderProvider is constructed.
 *   Cell encoders are cached and reused.
 * </p>
 */
@ApiAudience.Private
public final class CellEncoderProvider {

  /** Maps column names to encoders. */
  private final ImmutableMap<String, FijiCellEncoder> mEncoderMap;

  /**
   * Initializes a provider for cell encoders.
   *
   * @param tableURI URI of the table this provider is for.
   * @param layout the FijiTableLayout for which to provide cell encoders.
   * @param schemaTable the FijiSchemaTable from which to retrieve cell schemas.
   * @param factory the CellEncoderFactory with which to build cell encoders.
   * @throws IOException in case of an error reading from the schema table.
   */
  public CellEncoderProvider(
      final FijiURI tableURI,
      final FijiTableLayout layout,
      final FijiSchemaTable schemaTable,
      final FijiCellEncoderFactory factory)
      throws IOException {

    // Compute the set of all the column names (map-type families and fully-qualified columns from
    // group-type families).
    final Set<FijiColumnName> columns = Sets.newHashSet();
    for (FamilyLayout fLayout : layout.getFamilies()) {
      if (fLayout.isMapType()) {
        columns.add(FijiColumnName.create(fLayout.getName(), null));
      } else if (fLayout.isGroupType()) {
        for (ColumnLayout cLayout : fLayout.getColumns()) {
          columns.add(FijiColumnName.create(fLayout.getName(), cLayout.getName()));
        }
      } else {
        throw new InternalFijiError(
            String.format("Family '%s' is neither map-type nor group-type.", fLayout.getName()));
      }
    }

    // Pro-actively build cell encoders for all columns in the table:
    final Map<String, FijiCellEncoder> encoderMap = Maps.newHashMap();
    for (FijiColumnName column : columns) {
      final CellSpec cellSpec = layout.getCellSpec(column)
          .setColumnURI(
              FijiURI.newBuilder(tableURI).withColumnNames(ImmutableList.of(column)).build())
          .setSchemaTable(schemaTable)
          .setEncoderFactory(factory);

      final FijiCellEncoder encoder = cellSpec.getEncoderFactory().create(cellSpec);
      encoderMap.put(column.getName(), encoder);
    }
    mEncoderMap = ImmutableMap.copyOf(encoderMap);
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface

  /**
   * Gets a cell encoder for the specified column or (map-type) family.
   *
   * <p>
   *   When requesting a encoder for a column within a map-type family, the encoder for the
   *   entire map-type family will be returned.
   * </p>
   *
   * @param family Family of the column to look up.
   * @param qualifier Qualifier of the column to look up.
   *     Null means no qualifier, ie. get a encoder for a (map-type) family.
   * @return a cell encoder for the specified column.
   *     Null if the column does not exist or if a family level encoder is requested from a group
   *     type family.
   * @throws IOException on I/O error.
   */
  public FijiCellEncoder getEncoder(String family, String qualifier) throws IOException {
    final String column = (qualifier != null) ? (family + ":" + qualifier) : family;
    final FijiCellEncoder encoder = mEncoderMap.get(column);
    if (encoder != null) {
      // There already exists a encoder for this column:
      return encoder;
    }

    if (qualifier != null) {
      // There is no encoder for the specified fully-qualified column.
      // Try the family (this will only work for map-type families):
      return getEncoder(family, null);
    }

    return null;
  }
}
