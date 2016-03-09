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
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreClient;
import com.moz.fiji.mapreduce.kvstore.RequiredStores;
import com.moz.fiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import com.moz.fiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import com.moz.fiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import com.moz.fiji.mapreduce.kvstore.lib.SeqFileKeyValueStore;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.util.Resources;
import com.moz.fiji.schema.util.TestingFileUtils;

public class TestFijiMapReduceJobBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestFijiMapReduceJobBuilder.class);

  /** A line count mapper for testing. */
  public static class MyMapper extends FijiMapper<LongWritable, Text, Text, IntWritable>
      implements KeyValueStoreClient {

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      context.write(line, new IntWritable(1));
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return IntWritable.class;
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.just("mapperMap", EmptyKeyValueStore.get());
    }
  }

  /** A sum reducer for testing. */
  public static class MyReducer extends FijiReducer<Text, IntWritable, Text, IntWritable>
      implements KeyValueStoreClient {
    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(line, new IntWritable(sum));
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return IntWritable.class;
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.with("reducerMap", EmptyKeyValueStore.get());
    }
  }

  private Configuration mConf = null;
  private File mTempDir = null;

  @Before
  public final void setupTestFijiTransformJob() throws Exception {
    mTempDir = TestingFileUtils.createTempDir("fijimrtest", "dir");
    mConf = new Configuration();
    mConf.set("fs.default.name", "file://" + mTempDir);
  }

  @After
  public final void teardownTestFijiTransformJob() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
    mTempDir = null;
    mConf = null;
  }

  @Test
  public void testBuild() throws Exception {
    final FijiMapReduceJob job = FijiMapReduceJobBuilder.create()
        .withConf(mConf)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(
            new Path("/path/to/my/output"), 16))
        .build();

    final Job hadoopJob = job.getHadoopJob();
    assertEquals(TextInputFormat.class, hadoopJob.getInputFormatClass());
    assertEquals(MyMapper.class, hadoopJob.getMapperClass());
    assertEquals(MyReducer.class, hadoopJob.getReducerClass());
    assertEquals(16, hadoopJob.getNumReduceTasks());
    assertEquals(TextOutputFormat.class, hadoopJob.getOutputFormatClass());

    // KeyValueStore-specific checks here.
    Configuration confOut = hadoopJob.getConfiguration();
    assertEquals(2, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("mapperMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("reducerMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }

  @Test
  public void testBuildWithXmlKVStores() throws Exception {
    // Test that we can override default configuration KeyValueStores from an XML file.
    final InputStream xmlStores =
        Resources.openSystemResource("com.moz.fiji/mapreduce/test-kvstores.xml");

    // This file needs to exist before we build the job, or else
    // we can't build the job; it's referenced by a key-value store that checks
    // for its presence.
    final File tmpFile = new File("/tmp/foo.seq");
    if (tmpFile.createNewFile()) {
      // We created this temp file, we're responsible for deleting it.
      tmpFile.deleteOnExit();
    }

    LOG.info("Building job...");
    final FijiMapReduceJob job = FijiMapReduceJobBuilder.create()
        .withConf(mConf)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(
            new Path("/path/to/my/output"), 16))
        .withStoreBindings(xmlStores)
        .build();

    xmlStores.close();

    LOG.info("Verifying job configuration...");
    final Job hadoopJob = job.getHadoopJob();
    assertEquals(TextInputFormat.class, hadoopJob.getInputFormatClass());
    assertEquals(MyMapper.class, hadoopJob.getMapperClass());
    assertEquals(MyReducer.class, hadoopJob.getReducerClass());
    assertEquals(16, hadoopJob.getNumReduceTasks());
    assertEquals(TextOutputFormat.class, hadoopJob.getOutputFormatClass());

    // KeyValueStore-specific checks here.
    // We override mapperMap with a SeqFileKeyValueStore.
    Configuration confOut = hadoopJob.getConfiguration();
    assertEquals(2, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(SeqFileKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("mapperMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("reducerMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }

  @Test
  public void testLocalPathJarDir() {
    // Tests of MapReduceJobBuilder.addJarDirectory() (For FIJIMR-234)

    FijiMapReduceJobBuilder builder = FijiMapReduceJobBuilder.create();
    builder.addJarDirectory(new File("/tmp"));
    builder.addJarDirectory(new File("/"));
    builder.addJarDirectory(new File("."));
    builder.addJarDirectory(new File("some/subdir"));

    builder.addJarDirectory(new Path("/tmp"));
    builder.addJarDirectory(new Path("/"));
    builder.addJarDirectory(new Path("."));
    builder.addJarDirectory(new Path("./"));
    builder.addJarDirectory(new Path("some/subdir"));
    builder.addJarDirectory(new Path("./subdir"));
    builder.addJarDirectory(new Path(".."));

    builder.addJarDirectory(new Path("file:/tmp"));
    builder.addJarDirectory(new Path("file:///tmp"));
    builder.addJarDirectory(new Path("file:/"));
    builder.addJarDirectory(new Path("file:///"));

    builder.addJarDirectory("/tmp");
    builder.addJarDirectory("/");
    builder.addJarDirectory(".");
    builder.addJarDirectory("some/subdir");
    builder.addJarDirectory("./somedir");
  }
}
