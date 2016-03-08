/**
 * (c) Copyright 2015 WibiData, Inc.
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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiBufferedWriter;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiPartition;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.cassandra.CassandraFijiClientTest;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestCassandraFijiResultScanner extends CassandraFijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraFijiResultScanner.class);

  /**
   * Test that a partitioned Fiji scan correctly returns all values in the table.
   */
  @Test
  public void testScanPartitions() throws IOException {
    final int n = 100000;
    final FijiColumnName column = FijiColumnName.create("primitive", "string_column");

    final Fiji fiji = getFiji();
    fiji.createTable(
        FijiTableLayouts.getLayout("com.moz.fiji/schema/layout/all-types-no-counters-schema.json"));

    List<FijiResult<Utf8>> results = Lists.newArrayList();

    final FijiTable table = fiji.openTable("all_types_table");
    try {
      try (FijiBufferedWriter writer = table.getWriterFactory().openBufferedWriter()) {
        for (long i = 0; i < n; i++) {
          writer.put(
              table.getEntityId(i),
              column.getFamily(),
              column.getQualifier(),
              Long.toString(i));
        }
      }

      try (FijiTableReader reader = table.openTableReader()) {
        final Collection<? extends FijiPartition> partitions = table.getPartitions();
        final FijiDataRequest dataRequest =
            FijiDataRequest.create(column.getFamily(), column.getQualifier());

        for (FijiPartition partition : partitions) {
          results.addAll(
              ImmutableList.copyOf(reader.<Utf8>getFijiResultScanner(dataRequest, partition)));
        }
      }
    } finally {
      table.release();
    }

    Assert.assertEquals(results.size(), n);

    Collections.sort(
        results, new Comparator<FijiResult<Utf8>>() {
          @Override
          public int compare(final FijiResult<Utf8> kr1, final FijiResult<Utf8> kr2) {
            Long key1 = kr1.getEntityId().getComponentByIndex(0);
            Long key2 = kr2.getEntityId().getComponentByIndex(0);
            return key1.compareTo(key2);
          }
        });

    for (int i = 0; i < 10000; i++) {
      final FijiResult<Utf8> result = results.get(i);
      Assert.assertEquals(1, Iterables.size(result));
      Assert.assertEquals(Long.valueOf(i), result.getEntityId().<Long>getComponentByIndex(0));
    }
  }
}
