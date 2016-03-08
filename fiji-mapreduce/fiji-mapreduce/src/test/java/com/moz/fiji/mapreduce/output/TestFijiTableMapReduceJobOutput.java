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

import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestFijiTableMapReduceJobOutput extends FijiClientTest {

  /** Test that mapper speculative execution is disabled for FijiTableMapReduceJobOutput. */
  @Test
  public void testSpecExDisabled() throws Exception {
    final Fiji fiji = getFiji();
    final FijiTableLayout layout = FijiTableLayout.createUpdatedLayout(
        FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE),  null);
    fiji.createTable("table", layout);
    FijiURI tableURI = FijiURI.newBuilder(fiji.getURI()).withTableName("table").build();

    final Job job = new Job();
    new DirectFijiTableMapReduceJobOutput(tableURI).configure(job);

    final Configuration conf = job.getConfiguration();
    boolean isMapSpecExEnabled = conf.getBoolean("mapred.map.tasks.speculative.execution", true);
    assertFalse(isMapSpecExEnabled);
  }
}
