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
package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestHBaseFijiResultPerformance extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseFijiResultPerformance.class);

  private void setupPerformanceTest() throws IOException {
    final InstanceBuilder.TableBuilder tableBuilder = new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.ROW_DATA_TEST));

    final InstanceBuilder.FamilyBuilder familyBuilder =
        tableBuilder.withRow("foo").withFamily("map");
    for (int j = 0; j < 100; j++) {
      final InstanceBuilder.QualifierBuilder qualifierBuilder =
          familyBuilder.withQualifier(String.valueOf(j));
      for (int k = 1; k <= 1000; k++) {
        qualifierBuilder.withValue(k, 1);
      }
    }
    tableBuilder.build();
  }

  private void warmupPerformanceTest(
      final FijiTable table,
      final HBaseFijiTableReader reader,
      final FijiDataRequest request
  ) throws IOException {
    final FijiRowData warmupRowData = reader.get(table.getEntityId("foo"), request);
    for (FijiCell<Integer> cell : warmupRowData.<Integer>asIterable("map")) {
      cell.getData();
    }
    final FijiResult<Object> warmupResult = reader.getResult(table.getEntityId("foo"), request);
    try {
      final FijiResult<Integer> columnResult =
          warmupResult.narrowView(FijiColumnName.create("map"));
      try {
        for (final FijiCell<Integer> integerFijiCell : columnResult) {
          integerFijiCell.getData();
        }
      } finally {
        columnResult.close();
      }
    } finally {
      warmupResult.close();
    }
  }

  private void resultFromRowData(
      final HBaseFijiTableReader reader,
      final EntityId eid,
      final FijiDataRequest request
  ) throws IOException {
    final long rowDataToResultTime = System.nanoTime();
    final HBaseFijiRowData testRowData =
        (HBaseFijiRowData) reader.get(eid, request);
    final long rowDataEndTime = System.nanoTime();
    testRowData.asFijiResult();
    final long resultEndTime = System.nanoTime();
    LOG.info("built row data in {} milliseconds",
        (double) (rowDataEndTime - rowDataToResultTime) / 1000000);
    LOG.info("built result from row data in {} milliseconds",
        (double) (resultEndTime - rowDataEndTime) / 1000000);
  }

  private void allValuesMapFamily(
      final HBaseFijiTable table,
      final HBaseFijiTableReader reader,
      final FijiDataRequest request
  ) throws IOException {
    {
      final long rawHBaseStartTime = System.nanoTime();
      final HBaseDataRequestAdapter adapter =
          new HBaseDataRequestAdapter(request, table.getColumnNameTranslator());
      final Get get = adapter.toGet(table.getEntityId("foo"), table.getLayout());
      final HTableInterface hTable = table.openHTableConnection();
      try {
        final Result result = hTable.get(get);
      } finally {
        hTable.close();
      }
      LOG.info("raw hbase time = {} milliseconds",
          (double) (System.nanoTime() - rawHBaseStartTime) / 1000000);
    }
    {
      final long rowDataStartTime = System.nanoTime();
      final FijiRowData testRowData = reader.get(table.getEntityId("foo"), request);
      testRowData.containsCell("family", "qualifier", 1);
      LOG.info("built row data in {} milliseconds",
          (double) (System.nanoTime() - rowDataStartTime) / 1000000);
      int seen = 0;
      for (FijiCell<Integer> cell : testRowData.<Integer>asIterable("map")) {
        Object v = cell.getData();
        seen++;
      }
      LOG.info("row data all map family time (saw {} cells) = {} milliseconds",
          seen, (double) (System.nanoTime() - rowDataStartTime) / 1000000);
    }
    {
      final long resultStartTime = System.nanoTime();
      final FijiResult<Integer> testResult = reader.getResult(table.getEntityId("foo"), request);
      try {
        LOG.info(
            "built result in {} milliseconds",
            (double) (System.nanoTime() - resultStartTime) / 1000000);
        final long itstart = System.nanoTime();
        final Iterator<FijiCell<Integer>> it = testResult.iterator();
        LOG.info(
            "built iterator in {} milliseconds",
            (double) (System.nanoTime() - itstart) / 1000000);
        int seen = 0;
        while (it.hasNext()) {
          Object v = it.next().getData();
          seen++;
        }
        LOG.info(
            "result all map family time (saw {} cells) = {} milliseconds",
            seen, (double) (System.nanoTime() - resultStartTime) / 1000000);
      } finally {
        testResult.close();
      }
    }
  }

  private void singleValue(
      final HBaseFijiTable table,
      final HBaseFijiTableReader reader
  ) throws IOException {
    {
      final FijiDataRequest singletonRequest = FijiDataRequest.create("map", "10");
      final long rowDataStartTime = System.nanoTime();
      final FijiRowData testRowData = reader.get(table.getEntityId("foo"), singletonRequest);
      testRowData.containsCell("family", "qualifier", 1);
      final Integer value = testRowData.getMostRecentValue("map", "10");
      LOG.info("row data single value time = {} nanoseconds",
          (double) (System.nanoTime() - rowDataStartTime) / 1000000);

      final long resultStartTime = System.nanoTime();
      final FijiResult<Integer> testResult =
          reader.getResult(table.getEntityId("foo"), singletonRequest);
      try {
        final Integer value2 = FijiResult.Helpers.getFirstValue(testResult);
        LOG.info("result single value time = {} nanoseconds",
            (double) (System.nanoTime() - resultStartTime) / 1000000);

        Assert.assertEquals(value, value2);
      } finally {
        testResult.close();
      }
    }
  }

  private void paged(
      final HBaseFijiTableReader reader,
      final EntityId eid
  ) throws IOException {
    final FijiDataRequest pagedRequest = FijiDataRequest.builder().addColumns(
        ColumnsDef.create().withPageSize(10).withMaxVersions(10000).add("map", null)).build();
    {
      final FijiRowData warmupRowData = reader.get(eid, pagedRequest);
      for (FijiCell<Integer> cell : warmupRowData.<Integer>asIterable("map")) {
        cell.getData();
      }
      final FijiResult<Object> warmupResult = reader.getResult(eid, pagedRequest);
      try {
        for (FijiCell<Object> cell : warmupResult) {
          cell.getData();
        }
      } finally {
        warmupResult.close();
      }
      {
        final long resultStartTime = System.nanoTime();
        final FijiResult<Integer> testResult = reader.getResult(eid, pagedRequest);
        try {
          final Iterator<FijiCell<Integer>> it = testResult.iterator();
          int seen = 0;
          while (it.hasNext()) {
            Integer v = it.next().getData();
            seen++;
          }
          LOG.info(
              "paged result all map family time ({} cells) = {} nanoseconds",
              seen, System.nanoTime() - resultStartTime);
        } finally {
          testResult.close();
        }
      }
    }
  }

  // Disabled by default.
  //@Test
  public void performanceTest() throws IOException {
    setupPerformanceTest();

    final FijiDataRequest request = FijiDataRequest.builder().addColumns(
        ColumnsDef.create().withMaxVersions(10000).add("map", null)).build();
    final HBaseFijiTable table =
        HBaseFijiTable.downcast(getFiji().openTable("row_data_test_table"));
    try {
      final HBaseFijiTableReader reader = (HBaseFijiTableReader) table.openTableReader();
      try {
        warmupPerformanceTest(table, reader, request);

        resultFromRowData(reader, table.getEntityId("foo"), request);

        allValuesMapFamily(table, reader, request);

        singleValue(table, reader);

        paged(reader, table.getEntityId("foo"));
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
