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

import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HConstants;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiInstaller;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;

/**
 * Command-line tool to delete Fiji tables, rows, and cells.
 *
 * <h2>Examples:</h2>
 * Delete an entire table:
 * <pre>
 *   fiji delete --target=fiji://my-hbase/my-instance/my-table/
 * </pre>
 * Delete an entire row:
 * <pre>
 *   fiji delete --target=fiji://my-hbase/my-instance/my-table/ \
 *       --entity-id=my-entity-id --timestamp=all
 * </pre>
 * Delete a single version of a cell:
 * <pre>
 *   fiji delete --target=fiji://my-hbase/my-instance/my-table/ \
 *       my-column-family:my-column-qualifier/ --entity-id=my-entity-id \
 *       --timestamp=123456789
 *   fiji delete --target=fiji://my-hbase/my-instance/my-table/ \
 *       my-column-family:my-column-qualifier/ --entity-id=my-entity-id \
 *       --timestamp=latest
 * </pre>
 */
@ApiAudience.Private
public final class DeleteTool extends BaseTool {
  @Flag(name="target", usage="URI of the element(s) to delete. Valid scopes are: "
      + "entire Fiji instance, entire Fiji table, entire family/column or set of families/columns.")
  private String mTargetURIFlag = null;

  @Flag(name="entity-id", usage="Optional entity ID of a row to delete or to delete from."
      + " (requires a specified table in --target)")
  private String mEntityIdFlag = null;

  @Flag(name="timestamp", usage = "Timestamp specification, one of: "
      + "'<timestamp>' to delete cells with exactly this timestamp; "
      + "'latest' to delete the most recent cell only; "
      + "'upto:<timestamp>' to delete all cells with a timestamp older than this timestamp; "
      + "'all' to delete all cells."
      + "Timestamp are expressed in milliseconds since the Epoch.")
  private String mTimestampFlag = "all";

  /** URI of the element to delete. */
  private FijiURI mTargetURI = null;

  /** Timestamp selector mode. */
  private static enum TimestampMode {
    EXACT, LATEST, UPTO, ALL
  }

  /**
   * Timestamp, in milliseconds since the Epoch, combined with mTimestampMode to select cells.
   * Unused when mode is LATEST or ALL.
   */
  private Long mTimestamp = null;

  /** Mode according to which mTimestamp is applied. */
  private TimestampMode mTimestampMode = TimestampMode.LATEST;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "delete";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Delete fiji tables, rows, and cells.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** Prefix used when specifying timestamps up-to a given time. */
  private static final String TIMESTAMP_UPTO_PREFIX = "upto:";

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    Preconditions.checkArgument((mTargetURIFlag != null) && !mTargetURIFlag.isEmpty(),
        "Specify a target element to delete or to delete from with "
        + "--target=fiji://hbase-address/fiji-instance[/table[/family[:qualifier]]]");
    mTargetURI = FijiURI.newBuilder(mTargetURIFlag).build();
    Preconditions.checkArgument(mTargetURI.getInstance() != null,
        "Invalid target '{}': cannot delete HBase cluster. "
        + "Specify a Fiji instance with --target=fiji://hbase-address/fiji-instance",
        mTargetURI);

    if (mTimestampFlag != null) {
      if (mTimestampFlag.equals("latest")) {
        mTimestampMode = TimestampMode.LATEST;
      } else if (mTimestampFlag.startsWith(TIMESTAMP_UPTO_PREFIX)) {
        mTimestampMode = TimestampMode.UPTO;
        mTimestamp = Long.parseLong(mTimestampFlag.substring(TIMESTAMP_UPTO_PREFIX.length()));
      } else if (mTimestampFlag.equals("all")) {
        mTimestampMode = TimestampMode.ALL;
      } else {
        mTimestampMode = TimestampMode.EXACT;
        mTimestamp = Long.parseLong(mTimestampFlag);
      }
    }
  }

  /**
   * Delete cells from a given row.
   *
   * @param table Table containing the row to delete from.
   * @param entityId Entity ID of the row to delete from.
   * @param columns Set of columns to delete. Empty means "all columns".
   * @param tsMode Timestamp mode describing how to understand the parameter "timestamp".
   * @param timestamp Optional timestamp, in milliseconds since the Epoch.
   * @return tool exit code.
   * @throws Exception if there is an exception
   */
  private int deleteFromRow(
      FijiTable table,
      EntityId entityId,
      List<FijiColumnName> columns,
      TimestampMode tsMode,
      Long timestamp)
      throws Exception {

    final FijiTableWriter writer = table.openTableWriter();
    try {
      if (columns.isEmpty()) {
        // Row wide delete:
        switch (tsMode) {
        case UPTO: {
          if (mayProceed("Are you sure you want to delete all cells with timestamp <= %d"
              + " from row '%s' in table '%s'?",
              timestamp, entityId, table.getURI())) {
            writer.deleteRow(entityId, timestamp);
          }
          return SUCCESS;
        }
        case ALL: {
          if (mayProceed("Are you sure you want to delete row '%s' from table '%s'?",
              entityId, table.getURI())) {
            writer.deleteRow(entityId);
          }
          return SUCCESS;
        }
        case EXACT:
        case LATEST:
          throw new IllegalArgumentException(
              "Row-wide delete with exact or latest timestamp are not implemented.");
        default:
          throw new RuntimeException("Unhandled timestamp mode: " + tsMode);
        }

      } else {
        // Targeting a set of columns:
        // Normalize the columns, and partition families vs individual columns.
        final Set<String> families = Sets.newTreeSet();
        for (FijiColumnName column : columns) {
          if (!column.isFullyQualified()) {
            families.add(column.getFamily());
          }
        }

        final Set<FijiColumnName> groupColumns = Sets.newTreeSet();
        for (FijiColumnName column : columns) {
          // Do not include columns whose family is already specified for deletion:
          if (column.isFullyQualified() && !families.contains(column.getFamily())) {
            groupColumns.add(column);
          }
        }

        Preconditions.checkArgument(families.isEmpty()
            || ((tsMode != TimestampMode.EXACT) && (tsMode != TimestampMode.LATEST)),
            "Family-wide delete with exact or latest timestamp are not implemented.");

        switch (tsMode) {
        case EXACT: {
          Preconditions.checkState(families.isEmpty());
          if (!mayProceed("Are you sure you want to delete cell with timestamp %d of columns %s "
              + "from row '%s' in table '%s'?",
              timestamp, Joiner.on(",").join(columns), entityId, table.getURI())) {
            return SUCCESS;
          }
          for (FijiColumnName column : groupColumns) {
            writer.deleteCell(entityId, column.getFamily(), column.getQualifier(), timestamp);
          }
          break;
        }
        case LATEST: {
          Preconditions.checkState(families.isEmpty());
          if (!mayProceed("Are you sure you want to delete the most recent cells of columns %s "
              + "from row '%s' in table '%s'?",
              timestamp, Joiner.on(",").join(columns), entityId, table.getURI())) {
            return SUCCESS;
          }
          for (FijiColumnName column : groupColumns) {
            writer.deleteCell(
                entityId, column.getFamily(), column.getQualifier(), HConstants.LATEST_TIMESTAMP);
          }
          break;
        }
        case UPTO: {
          if (!mayProceed("Are you sure you want to delete all cells of columns %s "
              + "with timestamp <= %d from row '%s' in table '%s'?",
              Joiner.on(",").join(columns), timestamp, entityId, table.getURI())) {
            return SUCCESS;
          }
          for (String family : families) {
            writer.deleteFamily(entityId, family, timestamp);
          }
          for (FijiColumnName column : groupColumns) {
            writer.deleteColumn(entityId, column.getFamily(), column.getQualifier(), timestamp);
          }
          break;
        }
        case ALL: {
          if (!mayProceed("Are you sure you want to delete columns %s from row '%s' in table '%s'?",
              Joiner.on(",").join(columns), entityId, table.getURI())) {
            return SUCCESS;
          }
          for (String family : families) {
            writer.deleteFamily(entityId, family);
          }
          for (FijiColumnName column : groupColumns) {
            writer.deleteColumn(entityId, column.getFamily(), column.getQualifier());
          }
          break;
        }
        default:
          throw new RuntimeException("Unhandled timestamp mode: " + tsMode);
        }

        return SUCCESS;
      }

    } finally {
      writer.close();
    }
  }

  /**
   * Delete an entire table.
   *
   * @param fiji Fiji instance where the table to delete lives.
   * @param tableURI URI of the table to delete.
   * @return tool exit code.
   * @throws Exception on error.
   */
  private int deleteTable(Fiji fiji, FijiURI tableURI) throws Exception {
    if (isInteractive() && !inputConfirmation(
        String.format("Are you sure you want to delete Fiji table '%s'?", tableURI),
        tableURI.getTable())) {
      getPrintStream().println("Delete aborted.");
      return FAILURE;
    }
    fiji.deleteTable(tableURI.getTable());
    getPrintStream().println(String.format("Fiji table '%s' deleted.", tableURI));
    return SUCCESS;
  }

  /**
   * Deletes an entire Fiji instance.
   *
   * @param instanceURI The Fiji instance to delete.
   * @return tool exit code.
   * @throws Exception on error.
   */
  private int deleteInstance(FijiURI instanceURI) throws Exception {
    final Fiji fiji = Fiji.Factory.open(instanceURI);
    try {
      getPrintStream().println("WARNING: This instance contains the table(s):");
      for (String name : fiji.getTableNames()) {
        getPrintStream().println(name);
      }

      if (isInteractive() && !inputConfirmation(
          String.format("Are you sure you want to delete Fiji instance '%s'?", instanceURI),
          instanceURI.getInstance())) {
        getPrintStream().println("Delete aborted.");
        return FAILURE;
      }
    } finally {
      fiji.release();
    }

    FijiInstaller.get().uninstall(fiji.getURI(), getConf());
    getPrintStream().println(String.format("Fiji instance '%s' deleted.", fiji.getURI()));
    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mTargetURI.getTable() == null) {
      // No table specified: delete Fiji instance:
      return deleteInstance(mTargetURI);
    }

    final Fiji fiji = Fiji.Factory.open(mTargetURI, getConf());
    try {
      final List<FijiColumnName> columns = mTargetURI.getColumns();  // never null

      if (null == mEntityIdFlag) {
        // No specific row to delete or to delete from:

        if (columns.isEmpty()) {
          // No specific column targeted, delete the entire table:
          return deleteTable(fiji, mTargetURI);
        } else {
          // Delete entire families/columns in the table:
          throw new RuntimeException(
              "Deleting entire families/columns across all rows is not implemented");
        }

      } else {
        // Delete is targeting one specific row:
        final FijiTable table = fiji.openTable(mTargetURI.getTable());
        try {
          final EntityId entityId =
              ToolUtils.createEntityIdFromUserInputs(mEntityIdFlag, table.getLayout());
          return deleteFromRow(table, entityId, columns, mTimestampMode, mTimestamp);
        } finally {
          table.release();
        }
      }

    } finally {
      fiji.release();
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new DeleteTool(), args));
  }
}
