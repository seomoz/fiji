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

package com.moz.fiji.schema.layout;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.avro.CellSchema;
import com.moz.fiji.schema.avro.ColumnDesc;
import com.moz.fiji.schema.avro.CompressionType;
import com.moz.fiji.schema.avro.FamilyDesc;
import com.moz.fiji.schema.avro.LocalityGroupDesc;
import com.moz.fiji.schema.avro.RowKeyEncoding;
import com.moz.fiji.schema.avro.RowKeyFormat;
import com.moz.fiji.schema.avro.SchemaStorage;
import com.moz.fiji.schema.avro.SchemaType;
import com.moz.fiji.schema.avro.TableLayoutDesc;

/** Tests for FijiTableLayout. */
public class TestFijiTableLayoutChangeSchema {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFijiTableLayoutChangeSchema.class);

  private static final String TABLE_LAYOUT_VERSION = "layout-1.0";

  /** Reference layout descriptor with a single column: "family_name:column_name". */
  private final TableLayoutDesc mRefLayoutDesc = TableLayoutDesc.newBuilder()
      .setName("table_name")
      .setKeysFormat(RowKeyFormat.newBuilder()
          .setEncoding(RowKeyEncoding.HASH_PREFIX)
          .build())
      .setVersion(TABLE_LAYOUT_VERSION)
      .setLocalityGroups(Lists.newArrayList(
          LocalityGroupDesc.newBuilder()
          .setName("locality_group_name")
          .setInMemory(false)
          .setTtlSeconds(84600)
          .setMaxVersions(1)
          .setCompressionType(CompressionType.GZ)
          .setFamilies(Lists.newArrayList(
              FamilyDesc.newBuilder()
                  .setName("family_name")
                  .setColumns(Lists.newArrayList(
                      ColumnDesc.newBuilder()
                          .setName("column_name")
                          .setColumnSchema(CellSchema.newBuilder()
                               .setStorage(SchemaStorage.UID)
                               .setType(SchemaType.INLINE)
                               .setValue("\"string\"")
                               .build())
                          .build()))
                  .build()))
          .build()))
      .build();

  /** Reference layout with a single column: "family_name:column_name". */
  private final FijiTableLayout mRefLayout;

  public TestFijiTableLayoutChangeSchema() throws Exception {
    mRefLayout = FijiTableLayout.newLayout(mRefLayoutDesc);
  }

  /** Changing schema storage is not allowed and should fail. */
  @Test
  public void testChangeSchemaStorage() throws Exception {
    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(RowKeyFormat.newBuilder()
            .setEncoding(RowKeyEncoding.HASH_PREFIX)
            .build())
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.HASH)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"string\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    try {
      FijiTableLayout.createUpdatedLayout(desc2, mRefLayout);
      fail("Schema storage modifications changes should fail");
    } catch (InvalidLayoutException itl) {
      // Expected
      assertTrue(itl.getMessage().contains("Cell schema storage cannot be modified"));
      LOG.info("Expected error: " + itl);
    }
  }

  /** Final column schema cannot change. */
  @Test
  public void testChangeFinalColumnSchema() throws Exception {
    final TableLayoutDesc desc1 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(RowKeyFormat.newBuilder()
            .setEncoding(RowKeyEncoding.HASH_PREFIX)
            .build())
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.FINAL)
                                 .setType(SchemaType.COUNTER)
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    final FijiTableLayout refLayout = FijiTableLayout.newLayout(desc1);

    final TableLayoutDesc desc2 = TableLayoutDesc.newBuilder()
        .setName("table_name")
        .setKeysFormat(RowKeyFormat.newBuilder()
            .setEncoding(RowKeyEncoding.HASH_PREFIX)
            .build())
        .setVersion(TABLE_LAYOUT_VERSION)
        .setLocalityGroups(Lists.newArrayList(
            LocalityGroupDesc.newBuilder()
            .setName("locality_group_name")
            .setInMemory(false)
            .setTtlSeconds(84600)
            .setMaxVersions(1)
            .setCompressionType(CompressionType.GZ)
            .setFamilies(Lists.newArrayList(
                FamilyDesc.newBuilder()
                    .setName("family_name")
                    .setColumns(Lists.newArrayList(
                        ColumnDesc.newBuilder()
                            .setName("column_name")
                            .setColumnSchema(CellSchema.newBuilder()
                                 .setStorage(SchemaStorage.FINAL)
                                 .setType(SchemaType.INLINE)
                                 .setValue("\"int\"")
                                 .build())
                            .build()))
                    .build()))
            .build()))
        .build();
    try {
      FijiTableLayout.createUpdatedLayout(desc2, refLayout);
      fail("Counters are forever");
    } catch (InvalidLayoutException itl) {
      // Expected
      assertTrue(itl.getMessage().contains("Final column schema cannot be modified"));
      LOG.info("Expected error: " + itl);
    }
  }
}
