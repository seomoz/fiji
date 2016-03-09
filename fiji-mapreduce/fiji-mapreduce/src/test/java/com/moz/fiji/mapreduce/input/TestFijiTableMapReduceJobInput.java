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

package com.moz.fiji.mapreduce.input;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.FijiMRTestLayouts;
import com.moz.fiji.mapreduce.MapReduceJobInput;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.filter.StripValueRowFilter;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.TestingFileUtils;

public class TestFijiTableMapReduceJobInput extends FijiClientTest {
  private File mTempDir;
  private Path mTempPath;
  private FijiTable mTable;

  @Before
  public void setUp() throws Exception {
    mTempDir = TestingFileUtils.createTempDir("test", "dir");
    mTempPath = new Path("file://" + mTempDir);

    getConf().set("fs.defaultFS", mTempPath.toString());
    getConf().set("fs.default.name", mTempPath.toString());
    getFiji().createTable(FijiMRTestLayouts.getTestLayout());

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
  public void testConfigure() throws IOException {
    final Job job = new Job();

    // Request the latest 3 versions of column 'info:email':
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(3).add("info", "email");
    FijiDataRequest dataRequest = builder.build();

    // Read from 'here' to 'there':
    final EntityId startRow = HBaseEntityId.fromHBaseRowKey(Bytes.toBytes("here"));
    final EntityId limitRow = HBaseEntityId.fromHBaseRowKey(Bytes.toBytes("there"));
    final FijiRowFilter filter = new StripValueRowFilter();
    final FijiTableMapReduceJobInput.RowOptions rowOptions =
        FijiTableMapReduceJobInput.RowOptions.create(startRow, limitRow, filter);
    final MapReduceJobInput fijiTableJobInput =
        new FijiTableMapReduceJobInput(mTable.getURI(), dataRequest, rowOptions);
    fijiTableJobInput.configure(job);

    // Check that the job was configured correctly.
    final Configuration conf = job.getConfiguration();
    assertEquals(
        mTable.getURI(),
        FijiURI.newBuilder(conf.get(FijiConfKeys.FIJI_INPUT_TABLE_URI)).build());

    final FijiDataRequest decoded =
        (FijiDataRequest) SerializationUtils.deserialize(
            Base64.decodeBase64(conf.get(FijiConfKeys.FIJI_INPUT_DATA_REQUEST)));
    assertEquals(dataRequest, decoded);

    final String confStartRow = Base64.encodeBase64String(startRow.getHBaseRowKey());
    final String confLimitRow = Base64.encodeBase64String(limitRow.getHBaseRowKey());
    assertEquals(confStartRow, conf.get(FijiConfKeys.FIJI_START_ROW_KEY));
    assertEquals(confLimitRow, conf.get(FijiConfKeys.FIJI_LIMIT_ROW_KEY));

    assertEquals(filter.toJson().toString(), conf.get(FijiConfKeys.FIJI_ROW_FILTER));
  }
}
