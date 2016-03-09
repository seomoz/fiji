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

package com.moz.fiji.mapreduce.output;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.output.framework.FijiHFileOutputFormat;
import com.moz.fiji.mapreduce.platform.FijiMRPlatformBridge;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.hbase.HBaseFijiInstaller;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.layout.impl.ColumnId;

/** Tests for FijiHFileOutputFormat. */
public class TestFijiHFileOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(TestFijiHFileOutputFormat.class);

  /** Counter for fake instance IDs. */
  private static final AtomicLong FAKE_INSTANCE_COUNTER = new AtomicLong(0);

  /** NullWritable shortcut. */
  private static final NullWritable NW = NullWritable.get();

  /**
   * Makes a dummy byte array.
   *
   * @param value Byte value to repeat.
   * @param nbytes Number of bytes.
   * @return a byte array with the specified number of bytes and the specified byte value.
   */
  private static byte[] makeBytes(int value, int nbytes) {
    final byte[] bytes = new byte[nbytes];
    for (int i = 0; i < nbytes; ++i) {
      bytes[i] = (byte) value;
    }
    return bytes;
  }

  /**
   * Makes an HFile put entry (KeyValue writable-comparable).
   *
   * @param row Row key.
   * @param family HBase family (as a Fiji locality group column ID).
   * @param qualifier HBase qualifier.
   * @param timestamp Cell timestamp.
   * @param value Cell content bytes.
   * @return a new HFileKeyValue with the specified parameters.
   */
  private static HFileKeyValue entry(
      String row, ColumnId family, String qualifier, long timestamp, byte[] value) {
    return new HFileKeyValue(
        toBytes(row), family.toByteArray(), toBytes(qualifier), timestamp, value);
  }

  /**
   * Makes an HFile delete entry (KeyValue writable-comparable).
   *
   * @param row Row key.
   * @param family HBase family (as a Fiji locality group column ID).
   * @param qualifier HBase qualifier.
   * @param timestamp Cell timestamp.
   * @param type Cell type (put or one of the flavors of delete)
   * @return a new HFileKeyValue with the specified parameters.
   */
  private static HFileKeyValue entry(
      String row, ColumnId family, String qualifier, long timestamp,
      HFileKeyValue.Type type) {
    return new HFileKeyValue(
        toBytes(row), family.toByteArray(), toBytes(qualifier), timestamp, type,
        HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Loads an HFile content into a list of KeyValue entries.
   *
   * @param path Path of the HFile to load.
   * @param conf Configuration.
   * @return the content of the specified HFile, as an ordered list of KeyValue entries.
   * @throws IOException on I/O error.
   */
  private static List<KeyValue> loadHFile(Path path, Configuration conf) throws IOException {
    final FileSystem fs = path.getFileSystem(conf);
    final CacheConfig cacheConf = new CacheConfig(conf);
    //TODO(WIBI-1872): HBase 0.96 incompatible changes requiring a bridge.
    final HFile.Reader reader = HFile.createReader(fs, path, cacheConf, conf);
    final HFileScanner scanner = reader.getScanner(false, false);
    final List<KeyValue> kvs = Lists.newArrayListWithCapacity((int) reader.getEntries());
    boolean hasNext = scanner.seekTo();
    while (hasNext) {
      kvs.add(scanner.getKeyValue());
      hasNext = scanner.next();
    }
    reader.close();
    return kvs;
  }

  /**
   * Asserts the content of an HFile.
   *
   * @param path Path of the HFile to validate the content of.
   * @param values Expected KeyValue entries, in order.
   * @throws IOException on I/O error.
   */
  private void assertHFileContent(Path path, KeyValue... values) throws IOException {
    final FileSystem fs = path.getFileSystem(mConf);
    assertTrue(String.format("HFile '%s' does not exist.", path), fs.exists(path));
    final List<KeyValue> kvs = loadHFile(path, mConf);
    assertEquals(kvs.size(), values.length);
    for (int i = 0; i < values.length; ++i) {
      assertEquals(kvs.get(i), values[i]);
    }
  }

  private Configuration mConf;
  private FijiURI mTableURI;
  private Fiji mFiji;
  private File mTempDir;
  private FijiTableLayout mLayout;
  private FijiHFileOutputFormat mFormat;

  private ColumnId mDefaultLGId;
  private ColumnId mInMemoryLGId;

  @Before
  public void setUp() throws Exception {
    mConf = HBaseConfiguration.create();
    mTableURI = FijiURI.newBuilder(String.format(
        "fiji://.fake.%s/default/user", FAKE_INSTANCE_COUNTER.getAndIncrement())).build();

    mTempDir = File.createTempFile("test-" + System.currentTimeMillis() + "-", "");
    Preconditions.checkState(mTempDir.delete());
    Preconditions.checkState(mTempDir.mkdir());

    mConf.set("fs.defaultFS", "file://" + mTempDir.toString());
    mConf.set("mapreduce.output.fileoutputformat.outputdir", "file://" + mTempDir.toString());

    HBaseFijiInstaller.get().install(mTableURI, mConf);
    mFiji = Fiji.Factory.open(mTableURI);

    mLayout = FijiTableLayout.newLayout(FijiTableLayouts.getLayout(FijiTableLayouts.FULL_FEATURED));
    mFiji.createTable("user", mLayout);

    mDefaultLGId = mLayout.getLocalityGroupMap().get("default").getId();
    mInMemoryLGId = mLayout.getLocalityGroupMap().get("inMemory").getId();

    mConf.set(FijiConfKeys.FIJI_OUTPUT_TABLE_URI, mTableURI.toString());

    mFormat = new FijiHFileOutputFormat();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
  }

  @Test
  public void testMaxHFileSizeSameRow() throws Exception {
    final HFileKeyValue entry1 = entry("row-key", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    final HFileKeyValue entry2 = entry("row-key", mDefaultLGId, "b", 1L, makeBytes(0, 1024));

    mConf.setInt(FijiHFileOutputFormat.CONF_HREGION_MAX_FILESIZE, entry1.getLength() + 1);

    final TaskAttemptID taskAttemptId = FijiMRPlatformBridge.get().newTaskAttemptID(
        "jobTracker_jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = FijiMRPlatformBridge.get().newTaskAttemptContext(
            mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, FijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);
    writer.write(entry1, NW);
    writer.write(entry2, NW);
    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    final Path inMemoryDir = new Path(outputDir, mInMemoryLGId.toString());
    assertTrue(!fs.exists(inMemoryDir));

    assertHFileContent(new Path(defaultDir, "00000"), entry1.getKeyValue(), entry2.getKeyValue());
    assertFalse(fs.exists(new Path(defaultDir, "00001")));

    mFormat.getOutputCommitter(context).commitTask(context);
  }

  @Test
  public void testMaxHFileSizeNewRow() throws Exception {
    final HFileKeyValue entry1 = entry("row-key1", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    final HFileKeyValue entry2 = entry("row-key2", mDefaultLGId, "b", 1L, makeBytes(0, 1024));

    mConf.setInt(FijiHFileOutputFormat.CONF_HREGION_MAX_FILESIZE, entry1.getLength() + 1);

    final TaskAttemptID taskAttemptId = FijiMRPlatformBridge.get().newTaskAttemptID(
        "jobTracker_jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = FijiMRPlatformBridge.get().newTaskAttemptContext(
        mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, FijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);
    writer.write(entry1, NW);
    writer.write(entry2, NW);
    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    final Path inMemoryDir = new Path(outputDir, mInMemoryLGId.toString());
    assertFalse(fs.exists(inMemoryDir));

    assertHFileContent(new Path(defaultDir, "00000"), entry1.getKeyValue());
    assertHFileContent(new Path(defaultDir, "00001"), entry2.getKeyValue());
    assertFalse(fs.exists(new Path(defaultDir, "00002")));

    mFormat.getOutputCommitter(context).commitTask(context);
  }

  @Test
  public void testMultipleLayouts() throws Exception {
    final TaskAttemptID taskAttemptId = FijiMRPlatformBridge.get().newTaskAttemptID(
        "jobTracker_jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = FijiMRPlatformBridge.get().newTaskAttemptContext(
        mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, FijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);

    final HFileKeyValue defaultEntry =
        entry("row-key", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    writer.write(defaultEntry, NW);
    final HFileKeyValue inMemoryEntry =
        entry("row-key", mInMemoryLGId, "a", 1L, makeBytes(2, 1024));
    writer.write(inMemoryEntry, NW);

    try {
      // Test with an invalid locality group ID:
      final ColumnId invalid = new ColumnId(1234);
      assertTrue(!mLayout.getLocalityGroupIdNameMap().containsKey(invalid));
      writer.write(entry("row-key", invalid, "a", 1L, HConstants.EMPTY_BYTE_ARRAY), NW);
      fail("Output format did not fail on unknown locality group IDs.");
    } catch (IllegalArgumentException iae) {
      LOG.info("Expected error: " + iae);
    }

    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    final Path inMemoryDir = new Path(outputDir, mInMemoryLGId.toString());
    assertTrue(fs.exists(inMemoryDir));

    assertHFileContent(new Path(defaultDir, "00000"), defaultEntry.getKeyValue());
    assertHFileContent(new Path(inMemoryDir, "00000"), inMemoryEntry.getKeyValue());

    mFormat.getOutputCommitter(context).commitTask(context);
  }

  @Test
  public void testTombstonesInHFile() throws Exception {
    final HFileKeyValue put = entry("row-key1", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    final HFileKeyValue deleteCell =
      entry("row-key2", mDefaultLGId, "a", 1L, HFileKeyValue.Type.DeleteCell);
    final HFileKeyValue deleteColumn =
      entry("row-key3", mDefaultLGId, "a", 1L, HFileKeyValue.Type.DeleteColumn);
    final HFileKeyValue deleteFamily =
      entry("row-key4", mDefaultLGId, "a", 1L, HFileKeyValue.Type.DeleteFamily);

    final TaskAttemptID taskAttemptId = FijiMRPlatformBridge.get().newTaskAttemptID(
        "jobTracker_jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = FijiMRPlatformBridge.get().newTaskAttemptContext(
            mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, FijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);
    writer.write(put, NW);
    writer.write(deleteCell, NW);
    writer.write(deleteColumn, NW);
    writer.write(deleteFamily, NW);
    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    assertHFileContent(
      new Path(defaultDir, "00000"),
      put.getKeyValue(),
      deleteCell.getKeyValue(),
      deleteColumn.getKeyValue(),
      deleteFamily.getKeyValue());
    assertFalse(fs.exists(new Path(defaultDir, "00001")));

    mFormat.getOutputCommitter(context).commitTask(context);
  }
}
