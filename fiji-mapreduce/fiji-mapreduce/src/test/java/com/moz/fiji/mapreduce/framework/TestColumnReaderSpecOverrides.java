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

package com.moz.fiji.mapreduce.framework;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder;
import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.mapreduce.produce.ProducerContext;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.avro.EmptyRecord;
import com.moz.fiji.schema.avro.TestRecord1;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestColumnReaderSpecOverrides extends FijiClientTest {
  private static final FijiColumnName EMPTY = new FijiColumnName("family", "empty");
  private static final FijiColumnName RECORD1 = new FijiColumnName("family", "record1");

  /** Test table, owned by this test. */
  private FijiTable mTable;

  /** Table reader, owned by this test. */
  private FijiTableReader mReader;

  @Before
  public final void setupTestProducer() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout =
        FijiTableLayouts.getTableLayout(FijiTableLayouts.READER_SCHEMA_TEST);

    // Build the test records.
    final EmptyRecord row1Value = EmptyRecord.newBuilder().build();
    final EmptyRecord row2Value = EmptyRecord.newBuilder().build();

    // Populate the environment.
    new InstanceBuilder(getFiji())
        .withTable(layout.getName(), layout)
            .withRow("row1")
                .withFamily(EMPTY.getFamily())
                    .withQualifier(EMPTY.getQualifier()).withValue(row1Value)
            .withRow("row2")
                .withFamily(EMPTY.getFamily())
                    .withQualifier(EMPTY.getQualifier()).withValue(row2Value)
        .build();

    // Fill local variables.
    mTable = getFiji().openTable(layout.getName());
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestProducer() throws Exception {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
  }

  public static class SimpleProducer extends FijiProducer {
    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      final FijiDataRequestBuilder.ColumnsDef overrideColumnsDef;
      try {
        overrideColumnsDef = FijiDataRequestBuilder.ColumnsDef
            .create()
            .add(EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class));
      } catch (InvalidLayoutException e) {
        throw new FijiIOException(e);
      }

      return FijiDataRequest
          .builder()
          .addColumns(overrideColumnsDef)
          .build();
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return RECORD1.getName();
    }

    /** {@inheritDoc} */
    @Override
    public void produce(FijiRowData input, ProducerContext context) throws IOException {
      // Unpack the row.
      final TestRecord1 record = input.getMostRecentValue(EMPTY.getFamily(), EMPTY.getQualifier());

      // Write out the unpacked record (with the schema mask applied).
      context.put(record);
    }
  }

  @Test
  public void testColumnReaderSpecOverrides() throws Exception {
    // Run producer.
    final FijiMapReduceJob job = FijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(SimpleProducer.class)
        .withInputTable(mTable.getURI())
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    Assert.assertTrue(job.run());

    // Validate produced output.
    final FijiDataRequest outputRequest = FijiDataRequest
        .create(RECORD1.getFamily(), RECORD1.getQualifier());
    final FijiRowData row1 = mReader.get(mTable.getEntityId("row1"), outputRequest);
    final FijiRowData row2 = mReader.get(mTable.getEntityId("row2"), outputRequest);

    final TestRecord1 row1Expected = TestRecord1.newBuilder().build();
    final TestRecord1 row2Expected = TestRecord1.newBuilder().build();
    final TestRecord1 row1Actual =
        row1.getMostRecentValue(RECORD1.getFamily(), RECORD1.getQualifier());
    final TestRecord1 row2Actual =
        row2.getMostRecentValue(RECORD1.getFamily(), RECORD1.getQualifier());
    Assert.assertEquals(row1Expected, row1Actual);
    Assert.assertEquals(row2Expected, row2Actual);
  }
}
