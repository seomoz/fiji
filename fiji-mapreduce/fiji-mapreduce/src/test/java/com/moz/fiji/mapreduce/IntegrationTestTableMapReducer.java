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

package com.moz.fiji.mapreduce;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.moz.fiji.mapreduce.gather.FijiGatherJobBuilder;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.testlib.SimpleTableMapReducer;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;

/** Tests running a table map/reducer. */
public class IntegrationTestTableMapReducer extends AbstractFijiIntegrationTest {
  @Test
  public void testTableMapReducer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);
    // NOTE: fs should get closed, but because of a bug with FileSystem that causes it to close
    // other thread's filesystem objects we do not. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973

    final FijiURI uri = getFijiURI();
    final Fiji fiji = Fiji.Factory.open(uri, conf);
    try {
      final int nregions = 16;
      final TableLayoutDesc layout = FijiMRTestLayouts.getTestLayout();
      final String tableName = layout.getName();
      fiji.createTable(layout, nregions);

      final FijiTable table = fiji.openTable(tableName);
      try {
        final FijiTableWriter writer = table.openTableWriter();
        try {
          for (int i = 0; i < 10; ++i) {
            writer.put(table.getEntityId("row-" + i), "primitives", "int", i % 3);
          }
        } finally  {
          writer.close();
        }

        final Path output = new Path(fs.getUri().toString(),
            String.format("/%s-%s-%d/table-mr-output",
                getClass().getName(), mTestName.getMethodName(), System.currentTimeMillis()));

        final FijiMapReduceJob mrjob = FijiGatherJobBuilder.create()
            .withConf(conf)
            .withGatherer(SimpleTableMapReducer.TableMapper.class)
            .withReducer(SimpleTableMapReducer.TableReducer.class)
            .withInputTable(table.getURI())
            .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(table.getURI(), output, 16))
            .build();
        assertTrue(mrjob.run());
      } finally {
        table.release();
      }
    } finally {
      fiji.release();
    }
  }
}
