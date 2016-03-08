/**
 * (c) Copyright 2014 WibiData, Inc.
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
package com.moz.fiji.mapreduce.lib.produce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

public class TestIdentityProducer extends FijiClientTest {

  private static final String TABLE_NAME = "identity_producer_table";

  private FijiTable mTable = null;

  @Before
  public void setupTestIdentityProducer() throws IOException {
    final TableLayoutDesc layout = FijiTableLayouts.getLayout(
        "com.moz.fiji/mapreduce/lib/mapping/identity-producer-table.json");
    new InstanceBuilder(getFiji())
        .withTable(layout)
            .withRow("foo")
                .withFamily("group").withQualifier("column").withValue(1, "one")
                .withFamily("map").withQualifier("map1").withValue(10, "ten")
        .build();
    mTable = getFiji().openTable(TABLE_NAME);
  }

  @After
  public void cleanupTestIdentityProducer() throws IOException {
    mTable.release();
  }

  @Test
  public void testGroupColumn() throws Exception {
    final FijiURI tableUri =
        FijiURI.newBuilder(getFiji().getURI()).withTableName(TABLE_NAME).build();
    final Configuration conf = getConf();
    conf.set(IdentityProducer.CONF_INPUT, "group:column");
    conf.set(IdentityProducer.CONF_OUTPUT, "group:column2");

    final FijiMapReduceJob job = FijiProduceJobBuilder.create()
        .withInputTable(tableUri)
        .withProducer(IdentityProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(tableUri))
        .withConf(conf)
        .build();

    Assert.assertTrue(job.run());

    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest request = FijiDataRequest.create("group", "column2");
    final FijiTableReader reader = mTable.openTableReader();
    try {
      final String expected = "one";
      final String actual =
          reader.get(eid, request).getMostRecentValue("group", "column2").toString();
      Assert.assertEquals(expected, actual);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMapColumn() throws Exception {
    final FijiURI tableUri =
        FijiURI.newBuilder(getFiji().getURI()).withTableName(TABLE_NAME).build();
    final Configuration conf = getConf();
    conf.set(IdentityProducer.CONF_INPUT, "map:map1");
    conf.set(IdentityProducer.CONF_OUTPUT, "map:map2");

    final FijiMapReduceJob job = FijiProduceJobBuilder.create()
        .withInputTable(tableUri)
        .withProducer(IdentityProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(tableUri))
        .withConf(conf)
        .build();

    Assert.assertTrue(job.run());

    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest request = FijiDataRequest.create("map", "map2");
    final FijiTableReader reader = mTable.openTableReader();
    try {
      final String expected = "ten";
      final String actual =
          reader.get(eid, request).getMostRecentValue("map", "map2").toString();
      Assert.assertEquals(expected, actual);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMapFamily() throws Exception {
    final FijiURI tableUri =
        FijiURI.newBuilder(getFiji().getURI()).withTableName(TABLE_NAME).build();
    final Configuration conf = getConf();
    conf.set(IdentityProducer.CONF_INPUT, "map");
    conf.set(IdentityProducer.CONF_OUTPUT, "map2");

    final FijiMapReduceJob job = FijiProduceJobBuilder.create()
        .withInputTable(tableUri)
        .withProducer(IdentityProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(tableUri))
        .withConf(conf)
        .build();

    Assert.assertTrue(job.run());

    final EntityId eid = mTable.getEntityId("foo");
    final FijiDataRequest request = FijiDataRequest.create("map2");
    final FijiTableReader reader = mTable.openTableReader();
    try {
      final String expected = "ten";
      final String actual =
          reader.get(eid, request).getMostRecentValue("map2", "map1").toString();
      Assert.assertEquals(expected, actual);
    } finally {
      reader.close();
    }
  }
}
