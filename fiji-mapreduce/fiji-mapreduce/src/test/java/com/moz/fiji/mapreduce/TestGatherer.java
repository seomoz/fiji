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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.gather.GathererContext;
import com.moz.fiji.mapreduce.gather.FijiGatherJobBuilder;
import com.moz.fiji.mapreduce.gather.FijiGatherer;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

/** Runs a gatherer job in-process against a fake HBase instance. */
public class TestGatherer extends FijiClientTest {
  /**
   * Gatherer intended to run on the generic FijiMR test layout.
   */
  public static class TestingGatherer extends FijiGatherer<LongWritable, Text> {

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return LongWritable.class;
   }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      return FijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public void gather(FijiRowData row, GathererContext<LongWritable, Text> context)
        throws IOException {
      final Integer zipCode = row.getMostRecentValue("info", "zip_code");
      final String userId = Bytes.toString((byte[]) row.getEntityId().getComponentByIndex(0));
      context.write(new LongWritable(zipCode), new Text(userId));
    }
  }

  /** Test table, owned by this test. */
  private FijiTable mTable;

  @Before
  public final void setupTestGatherer() throws Exception {
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
                    .withQualifier("zip_code").withValue(94110)
        .build();

    // Fill local variables.
    mTable = getFiji().openTable("test");
  }

  @After
  public final void teardownTestGatherer() throws Exception {
    ResourceUtils.releaseOrLog(mTable);
  }

  @Test
  public void testGatherer() throws Exception {
    final File outputDir = File.createTempFile("gatherer-output", ".dir", getLocalTempDir());
    Preconditions.checkState(outputDir.delete());
    final int numSplits = 1;

    // Run gatherer:
    final FijiMapReduceJob job = FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(TestingGatherer.class)
        .withInputTable(mTable.getURI())
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(
            new Path(outputDir.toString()), numSplits))
        .build();
    assertTrue(job.run());

    // Validate output:
    final File outputPartFile = new File(outputDir, "part-m-00000");
    final String gatheredText = FileUtils.readFileToString(outputPartFile);
    final String[] lines = gatheredText.split("\n");
    assertEquals(2, lines.length);
    final Set<String> userIds = Sets.newHashSet();
    for (String line : lines) {
      final String[] split = line.split("\t");
      assertEquals(2, split.length);
      assertEquals("94110", split[0]);
      userIds.add(split[1]);
    }
    assertTrue(userIds.contains("Marsellus Wallace"));
    assertTrue(userIds.contains("Vincent Vega"));
  }
}
