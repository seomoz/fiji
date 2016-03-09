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

package com.moz.fiji.mapreduce.framework;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/** Configuration keys used by FijiMR in Hadoop Configuration objects. */
@ApiAudience.Public
@ApiStability.Evolving
public final class FijiConfKeys {

  /** URI of the input table to read from. */
  public static final String FIJI_INPUT_TABLE_URI = "fiji.input.table.uri";

  /** URI of the output Fiji table to write to. */
  public static final String FIJI_OUTPUT_TABLE_URI = "fiji.output.table.uri";

  /** Name of the FijiTableContext class to use. */
  public static final String FIJI_TABLE_CONTEXT_CLASS = "fiji.table.context.class";

  /** Serialized input data request. */
  public static final String FIJI_INPUT_DATA_REQUEST = "fiji.input.data.request";

  /** HBase start row key. */
  public static final String FIJI_START_ROW_KEY = "fiji.input.start.key";

  /** HBase limit row key. */
  public static final String FIJI_LIMIT_ROW_KEY = "fiji.input.limit.key";

  /** Serialized FijiRowFilter. */
  public static final String FIJI_ROW_FILTER = "fiji.input.row.filter";

  /** Fully qualified name of the {@link com.moz.fiji.mapreduce.tools.FijiGather} class to run. */
  public static final String FIJI_GATHERER_CLASS = "fiji.gatherer.class";

  /** Fully qualified name of the {@link com.moz.fiji.mapreduce.produce.FijiProducer} class to run. */
  public static final String FIJI_PRODUCER_CLASS = "fiji.producer.class";

  /**
   * Fully qualified name of the {@link com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter} class to
   * run.
   **/
  public static final String FIJI_BULK_IMPORTER_CLASS = "fiji.bulk.importer.class";

  /** Fully qualified name of the {@link com.moz.fiji.mapreduce.pivot.FijiPivoter} class to run. */
  public static final String FIJI_PIVOTER_CLASS = "fiji.pivoter.class";

  /** Fiji Instance Name. */
  public static final String FIJI_INSTANCE_NAME = "fiji.instance.name";

  /** Polling interval in milliseconds for Fiji MapReduce jobs. */
  public static final String FIJI_MAPREDUCE_POLL_INTERVAL = "fiji.mapreduce.poll.interval";

  /** Utility class may not be instantiated. */
  private FijiConfKeys() {
  }
}
