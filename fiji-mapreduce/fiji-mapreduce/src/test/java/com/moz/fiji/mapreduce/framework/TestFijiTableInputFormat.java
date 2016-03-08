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

package com.moz.fiji.mapreduce.framework;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.framework.HBaseFijiTableInputFormat.FijiTableRecordReader;
import com.moz.fiji.schema.FijiClientTest;

/** Runs a producer job in-process against a fake HBase instance. */
public class TestFijiTableInputFormat extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFijiTableInputFormat.class);

  @Test
  public void testBytesToPosition() {
    Assert.assertEquals(0, FijiTableRecordReader.bytesToPosition(new byte[]{}, 4));
    Assert.assertEquals(0, FijiTableRecordReader.bytesToPosition(new byte[]{0}, 4));
    Assert.assertEquals(0, FijiTableRecordReader.bytesToPosition(new byte[]{0, 0}, 4));
    Assert.assertEquals(0, FijiTableRecordReader.bytesToPosition(new byte[]{0, 0, 0}, 4));
    Assert.assertEquals(0, FijiTableRecordReader.bytesToPosition(new byte[]{0, 0, 0, 0}, 4));
    Assert.assertEquals(0, FijiTableRecordReader.bytesToPosition(new byte[]{0, 0, 0, 0, 10}, 4));

    Assert.assertEquals(
        0x01000000L, FijiTableRecordReader.bytesToPosition(new byte[]{1}, 4));
    Assert.assertEquals(
        0x01020304L, FijiTableRecordReader.bytesToPosition(new byte[]{1, 2, 3, 4, 5}, 4));
    Assert.assertEquals(
        0xffL << 24, FijiTableRecordReader.bytesToPosition(new byte[]{(byte) 0xff}, 4));
    Assert.assertEquals(
        0xffffffffL, FijiTableRecordReader.bytesToPosition(
            new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}, 4));
  }

  @Test
  public final void testComputeProgressFullRange() {
    final byte[] startRowKey = new byte[]{};
    final byte[] stopRowKey = new byte[]{};
    final long startPos = FijiTableRecordReader.getStartPos(startRowKey);
    final long stopPos = FijiTableRecordReader.getStopPos(stopRowKey);
    LOG.info("Start pos = {}", startPos);
    LOG.info("Stop pos = {}", stopPos);
    Assert.assertEquals(
        0.0f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{}),
        0.0);
    Assert.assertEquals(
        0.0 / 256,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{0}),
        0.0);
    Assert.assertEquals(
        1.0 / 256,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{1}),
        1e-6);
    Assert.assertEquals(
        255.0 / 256,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{(byte) 0xff}),
        1e-6);
  }

  @Test
  public final void testComputeProgressSubRange() {
    final byte[] startRowKey = new byte[]{10};
    final byte[] stopRowKey = new byte[]{12};
    final long startPos = FijiTableRecordReader.getStartPos(startRowKey);
    final long stopPos = FijiTableRecordReader.getStopPos(stopRowKey);
    LOG.info("Start pos = {}", startPos);
    LOG.info("Stop pos = {}", stopPos);
    Assert.assertEquals(
        0.0f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{10}),
        0.0);
    Assert.assertEquals(
        0.5f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{11}),
        1e-6);
    Assert.assertEquals(
        1.0f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{12}),
        0.0);
    Assert.assertEquals(
        0.0f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{10, 0}),
        0.0);
    Assert.assertEquals(
        0.25f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{10, (byte) 0x80}),
        1e-6);
    Assert.assertEquals(
        0.25f,
        FijiTableRecordReader.computeProgress(startPos, stopPos, new byte[]{10, (byte) 0x80}),
        1e-6);
  }

}
