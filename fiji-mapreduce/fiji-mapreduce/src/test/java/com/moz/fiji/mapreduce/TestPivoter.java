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

package com.moz.fiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.pivot.FijiPivotJobBuilder;
import com.moz.fiji.mapreduce.pivot.FijiPivoter;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.InstanceBuilder;

/** Runs a {@link FijiPivoter} job in-process against a fake HBase instance. */
public class TestPivoter extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestPivoter.class);

  // -----------------------------------------------------------------------------------------------

  /** {@link FijiPivoter} intended to run on the generic FijiMR test layout. */
  public static class TestingPivoter extends FijiPivoter {
    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      return FijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public void produce(FijiRowData row, FijiTableContext context)
        throws IOException {
      final Integer zipCode = row.getMostRecentValue("info", "zip_code");
      final String userId = Bytes.toString((byte[]) row.getEntityId().getComponentByIndex(0));

      final EntityId eid = context.getEntityId(zipCode.toString());
      context.put(eid, "primitives", "string", userId);
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Test table, owned by this test. */
  private FijiTable mTable;

  @Before
  public final void setupTest() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());

    // Populate the environment.
    new InstanceBuilder(getFiji())
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
                    .withQualifier("zip_code").withValue(94110)
            .withRow("Vincent Vega")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Vincent")
                    .withQualifier("last_name").withValue("Vega")
                    .withQualifier("zip_code").withValue(94111)
        .build();

    // Fill local variables.
    mTable = getFiji().openTable("test");
  }

  @After
  public final void teardownTest() throws Exception {
    mTable.release();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testPivoter() throws Exception {
    final FijiMapReduceJob job = FijiPivotJobBuilder.create()
        .withConf(getConf())
        .withPivoter(TestingPivoter.class)
        .withInputTable(mTable.getURI())
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());
    assertEquals(2,
        job.getHadoopJob().getCounters()
            .findCounter(JobHistoryCounters.PIVOTER_ROWS_PROCESSED).getValue());

    final FijiTableReader reader = mTable.openTableReader();
    try {
      final FijiDataRequest dataRequest = FijiDataRequest.create("primitives");
      final FijiRowData row1 = reader.get(mTable.getEntityId("94110"), dataRequest);
      assertEquals("Marsellus Wallace", row1.getMostRecentValue("primitives", "string").toString());

      final FijiRowData row2 = reader.get(mTable.getEntityId("94111"), dataRequest);
      assertEquals("Vincent Vega", row2.getMostRecentValue("primitives", "string").toString());

    } finally {
      reader.close();
    }
  }
}
