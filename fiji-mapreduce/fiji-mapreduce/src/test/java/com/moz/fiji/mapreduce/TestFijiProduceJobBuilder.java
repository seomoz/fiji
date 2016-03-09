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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.framework.HBaseFijiTableInputFormat;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.RequiredStores;
import com.moz.fiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import com.moz.fiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import com.moz.fiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import com.moz.fiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.output.framework.FijiHFileOutputFormat;
import com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder;
import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.mapreduce.produce.ProducerContext;
import com.moz.fiji.mapreduce.produce.impl.ProduceMapper;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;

public class TestFijiProduceJobBuilder extends FijiClientTest {

  /** A producer to use in the test job.  It writes "stuff" to family:column. */
  public static class MyProducer extends FijiProducer {
    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return "info:email";
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      return FijiDataRequest.create("info", "email");
    }

    /** {@inheritDoc} */
    @Override
    public void produce(FijiRowData input, ProducerContext context)
        throws IOException {
      context.put("stuff");
    }
  }

  /** Producer that requires a KeyValueStore but doesn't specify a good default. */
  public static class UnconfiguredKVProducer extends MyProducer {
    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.with("foostore", UnconfiguredKeyValueStore.get());
    }
  }

  /** Test table, owned by this test. */
  private FijiTable mTable;

  @Before
  public final void setupTestFijiProducer() throws Exception {
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());
    getFiji().createTable("test", layout);

    // Set the working directory so that it gets cleaned up after the test:
    getConf().set("mapred.working.dir", "file://" + getLocalTempDir() + "/workdir");

    mTable = getFiji().openTable("test");
  }

  @After
  public void teardownTestFijiProducer() throws Exception {
    ResourceUtils.releaseOrLog(mTable);
    mTable = null;
  }

  @Test
  public void testBuildWithHFileOutput() throws ClassNotFoundException, IOException {

    final FijiMapReduceJob produceJob = FijiProduceJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withProducer(MyProducer.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), new Path("foo/bar"), 10))
        .build();

    // Verify that the MR Job was configured correctly.
    final Job job = produceJob.getHadoopJob();
    assertEquals(HBaseFijiTableInputFormat.class, job.getInputFormatClass());
    assertEquals(ProduceMapper.class, job.getMapperClass());
    assertEquals(MyProducer.class,
        job.getConfiguration().getClass(FijiConfKeys.FIJI_PRODUCER_CLASS, null));
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(FijiHFileOutputFormat.class, job.getOutputFormatClass());
  }

  @Test
  public void testUnconfiguredKeyValueStore() throws ClassNotFoundException, IOException {
    // Should explode as we don't define a KVStore for 'foostore', but the class requires one

    // This should throw an exception because we didn't provide a better KVStore
    // than the UnconfiguredKeyValueStore in the default.
    try {
      FijiProduceJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withProducer(UnconfiguredKVProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Cannot use an UnconfiguredKeyValueStore. "
          + "You must override this on the command line or in a JobBuilder.",
          ioe.getMessage());
    }
  }

  @Test
  public void testEmptyKeyValueStore() throws ClassNotFoundException, IOException {
    // We override UnconfiguredKeyValueStore with EmptyKeyValueStore; this should succeed.
    FijiMapReduceJob produceJob = FijiProduceJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withProducer(UnconfiguredKVProducer.class)
        .withStore("foostore", EmptyKeyValueStore.get())
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mTable.getURI()))
        .build();

    // Verify that the MR Job was configured correctly.
    Job job = produceJob.getHadoopJob();
    Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }

  @Test
  public void testBuildWithDifferentTableOutput() throws ClassNotFoundException, IOException {
    final FijiTableLayout layout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout("other"));
    getFiji().createTable(layout.getName(), layout);

    final FijiTable otherTable = getFiji().openTable("other");
    try {
      // This should throw a job configuration exception because the output table does not
      // match the input table.
      FijiProduceJobBuilder.create()
          .withConf(getConf())
          .withInputTable(mTable.getURI())
          .withProducer(MyProducer.class)
          .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(otherTable.getURI()))
          .build();
      fail("Should have thrown a JobConfigurationException.");
    } catch (JobConfigurationException jce) {
      assertEquals("Output table must be the same as the input table.", jce.getMessage());
    } finally {
      ResourceUtils.releaseOrLog(otherTable);
    }
  }
}
