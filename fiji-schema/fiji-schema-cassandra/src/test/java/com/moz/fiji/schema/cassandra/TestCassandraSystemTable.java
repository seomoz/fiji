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

package com.moz.fiji.schema.cassandra;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.util.ProtocolVersion;

/** Tests for FijiSystemTable. */
public class TestCassandraSystemTable extends CassandraFijiClientTest {
  private static final String KEY = "some.system.property";
  private static final byte[] VALUE1 = Bytes.toBytes("value1");
  private static final byte[] VALUE2 = Bytes.toBytes("value2");

  @Test
  public void testStoreVersion() throws IOException {
    final Fiji fiji = getFiji();
    final FijiSystemTable systemTable = fiji.getSystemTable();
    final ProtocolVersion originalDataVersion = systemTable.getDataVersion();
    systemTable.setDataVersion(ProtocolVersion.parse("fiji-99"));

    assertEquals(ProtocolVersion.parse("fiji-99"), systemTable.getDataVersion());
    systemTable.setDataVersion(originalDataVersion);
  }

  @Test
  public void testPutGet() throws IOException {
    final Fiji fiji = getFiji();
    final FijiSystemTable systemTable = fiji.getSystemTable();

    assertNull(systemTable.getValue(KEY));

    systemTable.putValue(KEY, VALUE1);
    assertArrayEquals(VALUE1, systemTable.getValue(KEY));

    systemTable.putValue(KEY, VALUE2);
    assertArrayEquals(VALUE2, systemTable.getValue(KEY));
  }
}
