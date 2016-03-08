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

package com.moz.fiji.mapreduce.testlib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImportJobBuilder;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.input.FijiTableMapReduceJobInput.RowOptions;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;

/**
 * Example of a «table mapper» implemented as a bulk-importer that reads from a Fiji table.
 *
 * A table mapper reads from a Fiji table and writes to another Fiji table (or possible the same).
 *
 * Requires a custom job builder to configure the input format.
 */
public final class SimpleTableMapperAsBulkImporter
    extends FijiBulkImporter<EntityId, FijiRowData> {

  /** {@inheritDoc} */
  @Override
  public void produce(EntityId eid, FijiRowData row, FijiTableContext context) throws IOException {
    final Long filePos = row.getMostRecentValue("primitives", "long");
    if (filePos != null) {
      context.put(eid, "info", "zip_code", filePos);
    }
  }

  /**
   * Job launcher is required to configure the Fiji table input format.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final FijiURI uri = FijiURI.newBuilder("fiji://.env/test/test").build();
    final Fiji fiji = Fiji.Factory.open(uri, conf);
    final FijiTable table = fiji.openTable(uri.getTable());

    final FijiDataRequest dataRequest = FijiDataRequest.create("primitives");

    final FijiMapReduceJob mrjob = FijiBulkImportJobBuilder.create()
        .withBulkImporter(SimpleTableMapperAsBulkImporter.class)
        .withInput(MapReduceJobInputs.newFijiTableMapReduceJobInput(
            table.getURI(), dataRequest, RowOptions.create()))
        .withOutput(MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(table.getURI()))
        .build();
    if (!mrjob.run()) {
      System.err.println("Job failed");
      System.exit(1);
    } else {
      System.exit(0);
    }
  }
}
