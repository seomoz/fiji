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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.impl.DirectFijiTableWriterContext;
import com.moz.fiji.mapreduce.input.FijiTableMapReduceJobInput.RowOptions;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

/** Runs a map-only job in-process against a fake HBase instance. */
public class TestLaunchMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(TestLaunchMapReduce.class);

  /**
   * Example mapper intended to run on the generic FijiMR test layout. This test uses the resource
   * com.moz.fiji/mapreduce/layout/test.json
   */
  public static class ExampleMapper
      extends FijiMapper<EntityId, FijiRowData, HFileKeyValue, NullWritable> {

    private FijiTableContext mTableContext;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      Preconditions.checkState(mTableContext == null);
      mTableContext = DirectFijiTableWriterContext.create(context);
    }

    /** {@inheritDoc} */
    @Override
    protected void map(EntityId eid, FijiRowData row, Context hadoopContext)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(mTableContext);

      final String userId = Bytes.toString((byte[]) eid.getComponentByIndex(0));
      final EntityId genEId = mTableContext.getEntityId("generated row for " + userId);
      mTableContext.put(genEId, "primitives", "string", "generated content for " + userId);
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(mTableContext);
      mTableContext.close();
      mTableContext = null;
      super.cleanup(context);
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return HFileKeyValue.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return NullWritable.class;
    }
  }

  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;

  @Before
  public void setUp() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());

    // Populate the environment.
    mFiji = new InstanceBuilder()
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
            .withRow("Vincent Vega")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Vincent")
                    .withQualifier("last_name").withValue("Vega")
        .build();

    // Fill local variables.
    mTable = mFiji.openTable("test");
    mReader = mTable.openTableReader();
  }

  @After
  public void tearDown() throws Exception {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mFiji);
  }

  @Test
  public void testMapReduce() throws Exception {
    final Configuration jobConf = new Configuration();
    jobConf.set("mapreduce.jobtracker.address", "local");

    final String tmpDir = "file:///tmp/hdfs-testing-" + System.nanoTime();
    jobConf.set("fs.default.name", tmpDir);
    jobConf.set("fs.default.FS", tmpDir);

    // Run the transform (map-only job):
    final FijiMapReduceJob job = FijiMapReduceJobBuilder.create()
        .withConf(jobConf)
        .withMapper(ExampleMapper.class)
        .withInput(MapReduceJobInputs.newFijiTableMapReduceJobInput(
            mTable.getURI(), FijiDataRequest.create("info"), RowOptions.create()))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate the output table content:
    {
      final FijiRowScanner scanner = mTable.openTableReader().getScanner(
          FijiDataRequest.create("info"));
      for (FijiRowData row : scanner) {
        final EntityId eid = row.getEntityId();
        final String userId = Bytes.toString((byte[]) eid.getComponentByIndex(0));
        LOG.info("Row: {}", userId);
        if (!userId.startsWith("generated row for ")) {
          assertEquals(userId, String.format("%s %s",
              row.getMostRecentValue("info", "first_name"),
              row.getMostRecentValue("info", "last_name")));
        }
      }
      scanner.close();
    }
  }
}
