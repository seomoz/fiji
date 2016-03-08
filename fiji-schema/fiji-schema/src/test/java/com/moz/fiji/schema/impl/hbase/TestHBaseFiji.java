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

package com.moz.fiji.schema.impl.hbase;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableNotFoundException;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/** Tests for HBaseFiji. */
public class TestHBaseFiji extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseFiji.class);

  /** Tests Fiji.openTable() on a table that doesn't exist. */
  @Test
  public void testOpenUnknownTable() throws Exception {
    final Fiji fiji = getFiji();

    try {
      final FijiTable table = fiji.openTable("unknown");
      Assert.fail("Should not be able to open a table that does not exist!");
    } catch (FijiTableNotFoundException ktnfe) {
      // Expected!
      LOG.debug("Expected error: {}", ktnfe);
      Assert.assertEquals("unknown", ktnfe.getTableURI().getTable());
    }
  }

  @Test
  public void testDeletingFijiTableWithUsersDoesNotFail() throws Exception {
    final Fiji fiji = getFiji();
    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST);
    final String tableName = layoutDesc.getName();
    fiji.createTable(layoutDesc);
    final FijiTable table = fiji.openTable(tableName);
    try {
      fiji.deleteTable(tableName);
    } finally {
      table.release();
    }
  }
}
