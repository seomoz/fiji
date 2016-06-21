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

package com.moz.fiji.schema.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.DecodedCell;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.FijiCellEncoder;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.impl.DefaultFijiCellEncoderFactory;
import com.moz.fiji.schema.impl.hbase.HBaseDataRequestAdapter;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * Applies a FijiRowFilter to various row-savvy objects.
 *
 * <p>There are several limitations when filtering cells this way, as the filter relies on byte
 * comparisons, which does not play well with Avro records.</p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class FijiRowFilterApplicator {
  /** The row filter to be applied by this applicator. */
  private final FijiRowFilter mRowFilter;

  /** The layout of the table the row filter will be applied to. */
  private final FijiTableLayout mTableLayout;

  /** Schema table. */
  private final FijiSchemaTable mSchemaTable;

  /**
   * An implementation of FijiRowFilter.Context that translates fiji entityIds, column
   * names, and cell values to their HBase counterparts.
   */
  @ApiAudience.Private
  private final class FijiRowFilterContext extends FijiRowFilter.Context {
    private final HBaseColumnNameTranslator mColumnNameTranslator;

    /**
     * Constructs a FijiRowFilterContext.
     *
     * @param columnNameTranslator Column name translator for the table to apply filter to.
     */
    private FijiRowFilterContext(HBaseColumnNameTranslator columnNameTranslator) {
      mColumnNameTranslator = columnNameTranslator;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getHBaseRowKey(String fijiRowKey) {
      return Bytes.toBytes(fijiRowKey);
    }

    /** {@inheritDoc} */
    @Override
    public HBaseColumnName getHBaseColumnName(FijiColumnName fijiColumnName)
        throws NoSuchColumnException {
      return mColumnNameTranslator.toHBaseColumnName(fijiColumnName);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getHBaseCellValue(FijiColumnName column, DecodedCell<?> fijiCell)
        throws IOException {
      final CellSpec cellSpec = mColumnNameTranslator.getTableLayout().getCellSpec(column)
          .setSchemaTable(mSchemaTable);
      final FijiCellEncoder encoder = DefaultFijiCellEncoderFactory.get().create(cellSpec);
      return encoder.encode(fijiCell);
    }
  }

  /**
   * Creates a new <code>FijiRowFilterApplicator</code> instance.
   * This private constructor is used by the <code>create()</code> factory method.
   *
   * @param rowFilter The row filter to be applied.
   * @param tableLayout The layout of the table this filter applies to.
   * @param schemaTable The fiji schema table.
   */
  private FijiRowFilterApplicator(FijiRowFilter rowFilter, FijiTableLayout tableLayout,
      FijiSchemaTable schemaTable) {
    mRowFilter = rowFilter;
    mTableLayout = tableLayout;
    mSchemaTable = schemaTable;
  }

  /**
   * Creates a new <code>FijiRowFilterApplicator</code> instance.
   *
   * @param rowFilter The row filter to be applied.
   * @param schemaTable The fiji schema table.
   * @param tableLayout The layout of the table this filter applies to.
   * @return a new FijiRowFilterApplicator instance.
   */
  public static FijiRowFilterApplicator create(FijiRowFilter rowFilter, FijiTableLayout tableLayout,
      FijiSchemaTable schemaTable) {
    return new FijiRowFilterApplicator(rowFilter, tableLayout, schemaTable);
  }

  /**
   * Applies the row filter to an HBase scan object.
   *
   * <p>This will tell HBase region servers to filter rows on the server-side, so filtered
   * rows will not even need to get sent across the network back to the client.</p>
   *
   * @param scan An HBase scan descriptor.
   * @throws IOException If there is an IO error.
   */
  public void applyTo(Scan scan) throws IOException {
    // The filter might need to request data that isn't already requested by the scan, so add
    // it here if needed.
    try {
      // TODO: SCHEMA-444 Avoid constructing a new FijiColumnNameTranslator below.
      new HBaseDataRequestAdapter(
          mRowFilter.getDataRequest(),
          HBaseColumnNameTranslator.from(mTableLayout))
          .applyToScan(scan, mTableLayout);
    } catch (InvalidLayoutException e) {
      throw new InternalFijiError(e);
    }

    // Set the filter.
    final FijiRowFilter.Context context =
        new FijiRowFilterContext(HBaseColumnNameTranslator.from(mTableLayout));
    scan.setFilter(mRowFilter.toHBaseFilter(context));
  }
}
