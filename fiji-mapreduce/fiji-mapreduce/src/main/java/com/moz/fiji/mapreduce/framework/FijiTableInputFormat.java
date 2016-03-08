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

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.delegation.Lookups;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.hbase.HBaseFijiURI;

/** InputFormat for Hadoop MapReduce jobs reading from a Fiji table. */
@ApiAudience.Framework
@ApiStability.Stable
public abstract class FijiTableInputFormat
    extends InputFormat<EntityId, FijiRowData>
    implements Configurable {

  /** Static factory class for getting instances of the appropriate  FijiTableInputFormatFactory. */
  public static final class Factory {
    /**
     * Returns a FijiFactory for the appropriate type of Fiji (HBase or Cassandra), based on a URI.
     *
     * @param uri for the Fiji instance to build with the factory.
     * @return the default FijiFactory.
     */
    public static FijiTableInputFormatFactory get(FijiURI uri) {
      FijiTableInputFormatFactory instance;
      String scheme = uri.getScheme();
      if (scheme.equals(FijiURI.KIJI_SCHEME)) {
        scheme = HBaseFijiURI.HBASE_SCHEME;
      }
      synchronized (Fiji.Factory.class) {
        instance = Lookups
            .getNamed(FijiTableInputFormatFactory.class)
            .lookup(scheme);
        assert (null != instance);
      }
      return instance;
    }
  }

  /**
   * Configures a Hadoop M/R job to read from a given table.
   *
   * @param job Job to configure.
   * @param tableURI URI of the table to read from.
   * @param dataRequest Data request.
   * @param startRow Minimum row key to process. May be left null to indicate
   *     that scanning should start at the beginning of the table.
   * @param endRow Maximum row key to process. May be left null to indicate that
   *     scanning should continue to the end of the table.
   * @param filter Filter to use for scanning. May be left null.
   * @throws IOException on I/O error.
   */
  public static void configureJob(
      Job job,
      FijiURI tableURI,
      FijiDataRequest dataRequest,
      EntityId startRow,
      EntityId endRow,
      FijiRowFilter filter
  ) throws IOException {
    Preconditions.checkNotNull(job, "job must not be null");
    Preconditions.checkNotNull(tableURI, "tableURI must not be null");
    Preconditions.checkNotNull(dataRequest, "dataRequest must not be null");

    final Configuration conf = job.getConfiguration();

    // TODO: Check for jars config:
    // GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);

    // Write all the required values to the job's configuration object.
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(dataRequest));
    conf.set(FijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);
    conf.set(FijiConfKeys.KIJI_INPUT_TABLE_URI, tableURI.toString());
    if (null != startRow) {
      conf.set(FijiConfKeys.KIJI_START_ROW_KEY,
          Base64.encodeBase64String(startRow.getHBaseRowKey()));
    }
    if (null != endRow) {
      conf.set(FijiConfKeys.KIJI_LIMIT_ROW_KEY,
          Base64.encodeBase64String(endRow.getHBaseRowKey()));
    }
    if (null != filter) {
      conf.set(FijiConfKeys.KIJI_ROW_FILTER, filter.toJson().toString());
    }
  }
}
