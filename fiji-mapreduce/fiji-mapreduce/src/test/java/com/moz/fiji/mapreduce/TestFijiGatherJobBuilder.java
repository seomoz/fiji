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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.gather.GathererContext;
import com.moz.fiji.mapreduce.gather.FijiGatherJobBuilder;
import com.moz.fiji.mapreduce.gather.FijiGatherer;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.RequiredStores;
import com.moz.fiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import com.moz.fiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import com.moz.fiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import com.moz.fiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.output.framework.FijiHFileOutputFormat;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestFijiGatherJobBuilder extends FijiClientTest {
  // -----------------------------------------------------------------------------------------------

  /** Regular gatherer that emits (Text, Text) pairs. */
  public static class SimpleGatherer extends FijiGatherer<Text, Text> {
    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      return FijiDataRequest.create("info", "email");
    }

    /** {@inheritDoc} */
    @Override
    public void gather(FijiRowData input, GathererContext<Text, Text> context)
        throws IOException {
      // Gathering logic, unused in these tests.
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Gatherer that emits Fiji puts to HFiles. */
  public static class GatherToHFile extends FijiGatherer<HFileKeyValue, NullWritable> {
    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      return FijiDataRequest.create("info", "email");
    }

    /** {@inheritDoc} */
    @Override
    public void gather(FijiRowData input, GathererContext<HFileKeyValue, NullWritable> context)
        throws IOException {
      // Gathering logic, unused in these tests.
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

  // -----------------------------------------------------------------------------------------------

  /** Gatherer that requires a KV store. */
  public static class UnconfiguredKVGatherer extends SimpleGatherer {
    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.just("foostore", UnconfiguredKeyValueStore.builder().build());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Combiner to use in the test job. */
  public static class MyCombiner extends FijiReducer<Text, Text, Text, Text> {
    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Reducer to use in the test job. */
  public static class MyReducer extends FijiReducer<Text, Text, Text, Text> {
    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Dummy table reducer. */
  public static class ReducerToHFile extends FijiTableReducer<Text, Text> {

    /** {@inheritDoc} */
    @Override
    protected void reduce(Text key, Iterable<Text> values, FijiTableContext context)
        throws IOException {
      // Reducing logic, unused here.
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Test table, owned by this test. */
  private FijiTable mTable;

  @Before
  public void setUp() throws Exception {
    try {
      // Make doSetUp() errors visible:
      doSetUp();
    } catch (Exception thr) {
      thr.printStackTrace();
      throw thr;
    }
  }

  private void doSetUp() throws Exception {
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());
    getFiji().createTable("test", layout);

    // Set the working directory so that it gets cleaned up after the test:
    getConf().set("mapred.working.dir", "file://" + getLocalTempDir() + "/workdir");

    mTable = getFiji().openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    ResourceUtils.releaseOrLog(mTable);
    mTable = null;
  }

  private Path getLocalTestPath(String name) {
    return new Path("file://" + new File(getLocalTempDir(), name));
  }

  @Test
  public void testBuildValid() throws Exception {
    final FijiMapReduceJob gatherJob = FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withGatherer(SimpleGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(new Path("mypath"), 10))
        .build();

    // TODO: Verify that the MR Job was configured correctly.
    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(SimpleGatherer.class.getName(), conf.get(FijiConfKeys.FIJI_GATHERER_CLASS));
    assertEquals(MyCombiner.class, job.getCombinerClass());
    assertEquals(MyReducer.class, job.getReducerClass());
  }

  @Test
  public void testGatherToHFile() throws Exception {
    final FijiMapReduceJob gatherJob = FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withGatherer(GatherToHFile.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), getLocalTestPath("hfile"), 10))
        .build();

    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(GatherToHFile.class.getName(), conf.get(FijiConfKeys.FIJI_GATHERER_CLASS));
    assertEquals(null, job.getCombinerClass());
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(FijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(HFileKeyValue.class, job.getOutputKeyClass());
    assertEquals(NullWritable.class, job.getOutputValueClass());
  }

  @Test
  public void testGatherReducerToHFile() throws Exception {
    final FijiMapReduceJob gatherJob = FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withGatherer(SimpleGatherer.class)
        .withReducer(ReducerToHFile.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), getLocalTestPath("hfile"), 10))
        .build();

    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(SimpleGatherer.class.getName(), conf.get(FijiConfKeys.FIJI_GATHERER_CLASS));
    assertEquals(null, job.getCombinerClass());
    assertEquals(ReducerToHFile.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(SequenceFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(HFileKeyValue.class, job.getOutputKeyClass());
    assertEquals(NullWritable.class, job.getOutputValueClass());
  }

  @Test
  public void testUnconfiguredKeyValueStore() throws Exception {
    try {
      // Should explode as we don't define a KVStore for 'foostore', but the class requires one:
      FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withGatherer(UnconfiguredKVGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(new Path("mypath"), 10))
        .build();
      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Cannot use an UnconfiguredKeyValueStore. "
          + "You must override this on the command line or in a JobBuilder.",
          ioe.getMessage());
    }
  }

  @Test
  public void testEmptyKeyValueStore() throws Exception {
    // We override UnconfiguredKeyValueStore with EmptyKeyValueStore; this should succeed.
    final FijiMapReduceJob gatherJob = FijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withGatherer(UnconfiguredKVGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(new Path("mypath"), 10))
        .withStore("foostore", EmptyKeyValueStore.builder().build())
        .build();

    // Verify that the MR Job was configured correctly.
    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(1, conf.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        conf.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        conf.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }
}
