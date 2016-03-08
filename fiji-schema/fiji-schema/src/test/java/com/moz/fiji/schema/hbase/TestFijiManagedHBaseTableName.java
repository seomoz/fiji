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

package com.moz.fiji.schema.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestFijiManagedHBaseTableName {
  @Test
  public void testMetaTable() {
    FijiManagedHBaseTableName tableName = FijiManagedHBaseTableName.getMetaTableName("default");
    assertEquals("fiji.default.meta", tableName.toString());
    assertArrayEquals(Bytes.toBytes("fiji.default.meta"), tableName.toBytes());
  }

  @Test
  public void testSchemaTable() {
    final FijiManagedHBaseTableName hashTableName =
        FijiManagedHBaseTableName.getSchemaHashTableName("default");
    assertEquals("fiji.default.schema_hash", hashTableName.toString());
    assertArrayEquals(Bytes.toBytes("fiji.default.schema_hash"), hashTableName.toBytes());

    final FijiManagedHBaseTableName idTableName =
        FijiManagedHBaseTableName.getSchemaIdTableName("default");
    assertEquals("fiji.default.schema_id", idTableName.toString());
    assertArrayEquals(Bytes.toBytes("fiji.default.schema_id"), idTableName.toBytes());
  }

  @Test
  public void testSystemTable() {
    FijiManagedHBaseTableName tableName = FijiManagedHBaseTableName.getSystemTableName("default");
    assertEquals("fiji.default.system", tableName.toString());
    assertArrayEquals(Bytes.toBytes("fiji.default.system"), tableName.toBytes());
  }

  @Test
  public void testUserTable() {
    FijiManagedHBaseTableName tableName
        = FijiManagedHBaseTableName.getFijiTableName("default", "foo");
    assertEquals("fiji.default.table.foo", tableName.toString());
    assertArrayEquals(Bytes.toBytes("fiji.default.table.foo"), tableName.toBytes());
  }

  @Test
  public void testEquals() {
    FijiManagedHBaseTableName foo = FijiManagedHBaseTableName.getFijiTableName("default", "foo");
    FijiManagedHBaseTableName bar = FijiManagedHBaseTableName.getFijiTableName("default", "bar");
    FijiManagedHBaseTableName bar2 = FijiManagedHBaseTableName.getFijiTableName("default", "bar");

    assertFalse(foo.equals(bar));
    assertTrue(bar.equals(bar2));
  }

  @Test
  public void testHashCode() {
    FijiManagedHBaseTableName foo = FijiManagedHBaseTableName.getFijiTableName("default", "foo");
    FijiManagedHBaseTableName bar = FijiManagedHBaseTableName.getFijiTableName("default", "bar");
    FijiManagedHBaseTableName bar2 = FijiManagedHBaseTableName.getFijiTableName("default", "bar");

    assertFalse(foo.hashCode() == bar.hashCode());
    assertTrue(bar.hashCode() == bar2.hashCode());
  }
}
