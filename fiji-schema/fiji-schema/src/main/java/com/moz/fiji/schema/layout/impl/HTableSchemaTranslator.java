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

package com.moz.fiji.schema.layout.impl;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.avro.LocalityGroupDesc;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.hbase.FijiManagedHBaseTableName;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout;

/**
 * Translates between FijiTableLayouts and HTableDescriptors.
 *
 * <p>A Fiji table has a layout with locality groups, families, and columns.  An HTable
 * only has HColumns.  This classes maps between the Fiji layout components and the HTable
 * schema components, which ultimately determines how we map Fiji data onto an HTable.</p>
 */
@ApiAudience.Private
public final class HTableSchemaTranslator {
  /**
   * Creates a new <code>HTableSchemaTranslator</code> instance.
   */
  public HTableSchemaTranslator() {
  }

  /**
   * Translates a Fiji table layout into an HColumnDescriptor.
   *
   * @param fijiInstanceName The name of the Fiji instance the table lives in.
   * @param tableLayout The Fiji table layout.
   * @return The HTableDescriptor to use for storing the Fiji table data.
   */
  public HTableDescriptor toHTableDescriptor(String fijiInstanceName, FijiTableLayout tableLayout) {
    // Figure out the name of the table.
    final String tableName = tableLayout.getName();
    final FijiManagedHBaseTableName hbaseTableName =
        FijiManagedHBaseTableName.getFijiTableName(fijiInstanceName, tableName);
    final HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName.toString());
    TableLayoutDesc tableLayoutDesc = tableLayout.getDesc();

    if (tableLayoutDesc.getMaxFilesize() != null) {
        tableDescriptor.setMaxFileSize(tableLayoutDesc.getMaxFilesize());
    }
    if (tableLayoutDesc.getMemstoreFlushsize() != null) {
        tableDescriptor.setMemStoreFlushSize(tableLayoutDesc.getMemstoreFlushsize());
    }

    HBaseColumnNameTranslator translator = HBaseColumnNameTranslator.from(tableLayout);

    // Add the columns.
    for (LocalityGroupLayout localityGroup : tableLayout.getLocalityGroupMap().values()) {
      tableDescriptor.addFamily(toHColumnDescriptor(localityGroup, translator));
    }

    return tableDescriptor;
  }

  /**
   * Translates a Fiji locality group into an HColumnDescriptor.
   *
   * @param localityGroup A Fiji locality group.
   * @param hbaseColumnNameTranslator to convert the locality group into the HBase family.
   * @return The HColumnDescriptor to use for storing the data in the locality group.
   */
  private static HColumnDescriptor toHColumnDescriptor(
      final LocalityGroupLayout localityGroup,
      final HBaseColumnNameTranslator hbaseColumnNameTranslator
  ) {
    byte[] hbaseFamilyName = hbaseColumnNameTranslator.toHBaseFamilyName(localityGroup);

    LocalityGroupDesc groupDesc = localityGroup.getDesc();
    return new HColumnDescriptor(
        hbaseFamilyName,
        groupDesc.getMaxVersions(),
        groupDesc.getCompressionType().toString(),
        groupDesc.getInMemory(),
        true,  // block cache
        groupDesc.getBlockSize() != null ? groupDesc.getBlockSize()
          : HColumnDescriptor.DEFAULT_BLOCKSIZE,
        groupDesc.getTtlSeconds(),
        groupDesc.getBloomType() != null ? groupDesc.getBloomType().toString()
          : HColumnDescriptor.DEFAULT_BLOOMFILTER,
        HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
  }
}
