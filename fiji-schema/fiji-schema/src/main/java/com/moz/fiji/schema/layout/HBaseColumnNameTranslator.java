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

package com.moz.fiji.schema.layout;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.layout.impl.hbase.HBaseNativeColumnNameTranslator;
import com.moz.fiji.schema.layout.impl.hbase.IdentityColumnNameTranslator;
import com.moz.fiji.schema.layout.impl.hbase.ShortColumnNameTranslator;

/**
 * Translates between HBase and Fiji table column names.
 *
 * <p>This abstract class defines an interface for mapping between names of HBase HTable
 * families/qualifiers and Fiji table family/qualifiers.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
public abstract class HBaseColumnNameTranslator {
  /**
   * Creates a new {@link HBaseColumnNameTranslator} instance.  Supports either
   * {@link ShortColumnNameTranslator}, {@link IdentityColumnNameTranslator}, or
   * {@link HBaseNativeColumnNameTranslator} based on the table layout.
   *
   * @param tableLayout The layout of the table to translate column names for.
   * @return {@link HBaseColumnNameTranslator} of the appropriate type.
   */
  public static HBaseColumnNameTranslator from(FijiTableLayout tableLayout) {
    switch (tableLayout.getDesc().getColumnNameTranslator()) {
      case SHORT:
        return new ShortColumnNameTranslator(tableLayout);
      case IDENTITY:
        return new IdentityColumnNameTranslator(tableLayout);
      case HBASE_NATIVE:
        return new HBaseNativeColumnNameTranslator(tableLayout);
      default:
        throw new UnsupportedOperationException(String.format(
            "Unsupported HBaseColumnNameTranslator: %s for column: %s.",
            tableLayout.getDesc().getColumnNameTranslator(),
            tableLayout.getName()));
    }
  }

  /**
   * Translates an HBase column name to a Fiji column name.
   *
   * @param hbaseColumnName The HBase column name.
   * @return The Fiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract FijiColumnName toFijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a Fiji column name into an HBase column name.
   *
   * @param fijiColumnName The Fiji column name.
   * @return The HBase column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract HBaseColumnName toHBaseColumnName(FijiColumnName fijiColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a Fiji LocalityGroup into an HBase family name.
   *
   * @param localityGroup The Fiji locality group.
   * @return The HBase column name.
   */
  public abstract byte[] toHBaseFamilyName(FijiTableLayout.LocalityGroupLayout localityGroup);

  /**
   * @return the table layout.
   */
  public abstract FijiTableLayout getTableLayout();
}
