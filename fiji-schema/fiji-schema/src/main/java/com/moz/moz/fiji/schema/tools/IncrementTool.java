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

package com.moz.fiji.schema.tools;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.CellSchema;
import com.moz.fiji.schema.avro.SchemaType;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Command-line tool to increment a counter in a cell of a fiji table.
 */
@ApiAudience.Private
public final class IncrementTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementTool.class);

  @Flag(name="cell", usage="URI of the cell(s) (qualified column) to increment, "
      + "such as --cell=fiji://hbase-address/fiji-instance/table/family:qualifier.")
  private String mCellURIFlag;

  @Flag(name="entity-id", usage="Row entity ID specification.")
  private String mEntityId;

  @Flag(name="value", usage="Integer amount to add to the counter(s). May be negative.")
  private int mValue = 1;

  /** URI specifying qualified columns with counters to increment. */
  private FijiURI mCellURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "increment";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Increment a counter column in a fiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    Preconditions.checkArgument(null != mCellURIFlag, "Specify a cell address with "
        + "--cell=fiji://hbase-address/fiji-instance/table/family:qualifier");
    mCellURI = FijiURI.newBuilder(mCellURIFlag).build();

    Preconditions.checkArgument(null != mEntityId, "Specify an entity ID with --entity-id=...");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Fiji fiji = Fiji.Factory.open(mCellURI, getConf());
    try {
      final FijiTable table = fiji.openTable(mCellURI.getTable());
      try {
        final FijiTableWriter writer = table.openTableWriter();
        try {
          for (FijiColumnName column : mCellURI.getColumns()) {
            try {
              final CellSchema cellSchema = table.getLayout().getCellSchema(column);
              if (cellSchema.getType() != SchemaType.COUNTER) {
                LOG.error("Can't increment non counter-type column '{}'", column);
                return FAILURE;
              }
              final EntityId entityId =
                  ToolUtils.createEntityIdFromUserInputs(mEntityId, table.getLayout());
              writer.increment(entityId, column.getFamily(), column.getQualifier(), mValue);

            } catch (IOException ioe) {
              LOG.error("Error while incrementing column '{}'", column);
              return FAILURE;
            }
          }
          return SUCCESS;

        } finally {
          ResourceUtils.closeOrLog(writer);
        }
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new IncrementTool(), args));
  }
}
