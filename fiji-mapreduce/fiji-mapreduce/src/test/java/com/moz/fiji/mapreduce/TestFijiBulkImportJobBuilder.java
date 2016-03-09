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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.bulkimport.impl.BulkImportMapper;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.RequiredStores;
import com.moz.fiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import com.moz.fiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import com.moz.fiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.output.framework.FijiHFileOutputFormat;
import com.moz.fiji.mapreduce.reducer.IdentityReducer;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.TestingFileUtils;

public class TestFijiBulkImportJobBuilder extends FijiClientTest {
  /** A FijiBulkImporter for testing that the job-builder propagates this class correctly. */
  public static class NoopBulkImporter extends FijiBulkImporter<Void, Void> {
    /** {@inheritDoc} */
    @Override
    public void produce(Void key, Void value, FijiTableContext context)
        throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /** A BulkImporter implementation that uses a KeyValueStore. */
  public static class KVStoreBulkImporter extends NoopBulkImporter {
    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.just("foostore", EmptyKeyValueStore.get());
    }
  }

  private File mTempDir;
  private Path mTempPath;
  private FijiTable mTable;

  @Before
  public void setUp() throws Exception {
    mTempDir = TestingFileUtils.createTempDir("test", "dir");
    mTempPath = new Path("file://" + mTempDir);

    getConf().set("fs.defaultFS", mTempPath.toString());
    getConf().set("fs.default.name", mTempPath.toString());
    final FijiTableLayout layout = FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());
    getFiji().createTable("test", layout);

    // Set the working directory so that it gets cleaned up after the test:
    getConf().set("mapred.working.dir", new Path(mTempPath, "workdir").toString());

    mTable = getFiji().openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
    ResourceUtils.releaseOrLog(mTable);
    mTempDir = null;
    mTempPath = null;
    mTable = null;
  }

  @Test
  public void testBuildWithHFileOutput() throws Exception {
    final FijiMapReduceJob mrjob = FijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(mTempPath, "input")))
        .withBulkImporter(NoopBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), new Path(mTempPath, "output"), 10))
        .build();

    final Job job = mrjob.getHadoopJob();
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(BulkImportMapper.class, job.getMapperClass());
    assertEquals(NoopBulkImporter.class,
        job.getConfiguration().getClass(FijiConfKeys.FIJI_BULK_IMPORTER_CLASS, null));
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(FijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(TotalOrderPartitioner.class, job.getPartitionerClass());
  }

  @Test
  public void testBuildWithKeyValueStore() throws Exception {
    final FijiMapReduceJob mrjob = FijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(mTempPath, "input")))
        .withBulkImporter(KVStoreBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), new Path(mTempPath, "output"), 10))
        .build();

    final Job job = mrjob.getHadoopJob();
    // Verify that everything else is what we expected as in the previous test
    // (except the bulk importer class name)...
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(BulkImportMapper.class, job.getMapperClass());
    assertEquals(KVStoreBulkImporter.class,
        job.getConfiguration().getClass(FijiConfKeys.FIJI_BULK_IMPORTER_CLASS, null));
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(FijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(TotalOrderPartitioner.class, job.getPartitionerClass());

    // KeyValueStore-specific checks here.
    final Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }
}
