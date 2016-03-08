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

package com.moz.fiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.gather.GathererContext;
import com.moz.fiji.mapreduce.gather.FijiGatherJobBuilder;
import com.moz.fiji.mapreduce.gather.FijiGatherer;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.platform.FijiMRPlatformBridge;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestRegexQualifierColumnFilter extends FijiClientTest {
  /** Test table, owned by this test. */
  private FijiTable mTable;

  @Before
  public final void setupTestRegexQualifierColumnFilter() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiTableLayouts.getLayout(FijiTableLayouts.REGEX));

    // Populate the environment.
    new InstanceBuilder(getFiji())
        .withTable("regex_test", layout)
            .withRow("1")
                .withFamily("family")
                    .withQualifier("apple").withValue("cell")
                    .withQualifier("banana").withValue("cell")
                    .withQualifier("carrot").withValue("cell")
                    .withQualifier("aardvark").withValue("cell")
        .build();

    // Fill local variables.
    mTable = getFiji().openTable("regex_test");
  }

  @After
  public final void teardownTestRegexQualifierColumnFilter() throws Exception {
    ResourceUtils.releaseOrLog(mTable);
  }

  /**
   * A test gatherer that outputs all qualifiers from the 'family' family that start with
   * the letter 'a'.
   */
  public static class MyGatherer extends FijiGatherer<Text, NullWritable> {
    @Override
    public FijiDataRequest getDataRequest() {
      FijiDataRequestBuilder builder = FijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(10)
          .withFilter(new RegexQualifierColumnFilter("a.*"))
          .addFamily("family");
      return builder.build();
    }

    @Override
    public void gather(FijiRowData input, GathererContext<Text, NullWritable> context)
        throws IOException {
      NavigableMap<String, NavigableMap<Long, CharSequence>> qualifiers =
          input.getValues("family");
      for (String qualifier : qualifiers.keySet()) {
        context.write(new Text(qualifier), NullWritable.get());
      }
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return NullWritable.class;
    }
  }

  @Test
  public void testRegexQualifierColumnFilter() throws Exception {
    final File outputDir = File.createTempFile("gatherer-output", ".dir", getLocalTempDir());
    Preconditions.checkState(outputDir.delete());
    final int numSplits = 1;

    // Run a gatherer over the test_table.
    final FijiMapReduceJob gatherJob = FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withGatherer(MyGatherer.class)
        .withOutput(MapReduceJobOutputs.newSequenceFileMapReduceJobOutput(
            new Path(outputDir.getPath()), numSplits))
        .build();
    assertTrue(gatherJob.run());

    // Check the output file: two things should be there (apple, aardvark).
    final SequenceFile.Reader reader = FijiMRPlatformBridge.get().newSeqFileReader(
        getConf(), new Path(outputDir.getPath(), "part-m-00000"));
    try {
      final Text key = new Text();
      assertTrue(reader.next(key));
      assertEquals("aardvark", key.toString());
      assertTrue(reader.next(key));
      assertEquals("apple", key.toString());
      assertFalse(reader.next(key));
    } finally {
      reader.close();
    }
  }
}
