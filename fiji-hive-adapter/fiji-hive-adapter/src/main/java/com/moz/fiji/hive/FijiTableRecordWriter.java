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

package com.moz.fiji.hive;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.io.FijiCellWritable;
import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.tools.ToolUtils;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Writes key-value records from a FijiTableInputSplit (usually 1 region in an HTable).
 */
public class FijiTableRecordWriter
    implements FileSinkOperator.RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableRecordWriter.class);

  private final Fiji mFiji;
  private final FijiTable mFijiTable;
  private final FijiTableWriter mFijiTableWriter;

  /**
   * Constructor.
   *
   * @param conf The job configuration.
   * @throws java.io.IOException If the input split cannot be opened.
   */
  public FijiTableRecordWriter(Configuration conf)
      throws IOException {
    String fijiURIString = conf.get(FijiTableOutputFormat.CONF_KIJI_TABLE_URI);
    FijiURI fijiURI = FijiURI.newBuilder(fijiURIString).build();
    mFiji = Fiji.Factory.open(fijiURI);
    mFijiTable = mFiji.openTable(fijiURI.getTable());
    mFijiTableWriter = mFijiTable.openTableWriter();
  }

  @Override
  public void close(boolean abort) throws IOException {
    ResourceUtils.closeOrLog(mFijiTableWriter);
    ResourceUtils.releaseOrLog(mFijiTable);
    ResourceUtils.releaseOrLog(mFiji);
  }

  @Override
  public void write(Writable writable) throws IOException {
    Preconditions.checkArgument(writable instanceof FijiRowDataWritable,
        "FijiTableRecordWriter can only operate on FijiRowDataWritable objects.");

    FijiRowDataWritable fijiRowDataWritable = (FijiRowDataWritable) writable;
    FijiTableLayout fijiTableLayout = mFijiTable.getLayout();

    // TODO(KIJIHIVE-30) Process EntityId components here as well.
    EntityId entityId = ToolUtils.createEntityIdFromUserInputs(
        fijiRowDataWritable.getEntityId().toShellString(),
        fijiTableLayout);

    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> writableData =
        fijiRowDataWritable.getData();
    for (FijiColumnName fijiColumnName: writableData.keySet()) {
      String family = fijiColumnName.getFamily();
      String qualifier = fijiColumnName.getQualifier();

      NavigableMap<Long, FijiCellWritable> timeseries = writableData.get(fijiColumnName);
      // Ignoring the redundant timestamp in this Map in favor of the one contained in the cell.
      for (FijiCellWritable fijiCellWritable : timeseries.values()) {
        Long timestamp = fijiCellWritable.getTimestamp();
        Schema schema = fijiCellWritable.getSchema();
        Preconditions.checkNotNull(schema);
        Object data = fijiCellWritable.getData();
        switch (schema.getType()) {
          case NULL:
            // Don't write null values.
            break;
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case BYTES:
          case FIXED:
            // Write the primitive type to Fiji.
            mFijiTableWriter.put(entityId, family, qualifier, timestamp, data);
            break;
          case RECORD:
          case ARRAY:
          case MAP:
          case UNION:
          default:
            // TODO(KIJIHIVE-31): Support the writing of some of these complex types.
            throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
        }
      }
    }
  }
}
