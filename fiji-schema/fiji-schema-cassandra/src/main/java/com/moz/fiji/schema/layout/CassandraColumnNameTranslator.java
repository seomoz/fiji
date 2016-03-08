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
import com.moz.fiji.schema.cassandra.CassandraColumnName;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.layout.impl.cassandra.ShortColumnNameTranslator;

/**
 * Translates between Cassandra and Fiji table column names.
 *
 * <p>This abstract class defines an interface for mapping between names of Cassandra HTable
 * families/qualifiers and Fiji table family/qualifiers.</p>
 *
 * TODO: Update Cassandra column name translators with identity and native.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public abstract class CassandraColumnNameTranslator {
  /**
   * Creates a new {@link CassandraColumnNameTranslator} instance. Supports the
   * {@link ShortColumnNameTranslator} based on the table layout.
   *
   * @param tableLayout The layout of the table to translate column names for.
   * @return {@link CassandraColumnNameTranslator} of the appropriate type.
   */
  public static CassandraColumnNameTranslator from(FijiTableLayout tableLayout) {
    switch (tableLayout.getDesc().getColumnNameTranslator()) {
      case SHORT:
        return new ShortColumnNameTranslator(tableLayout);
      default:
        throw new UnsupportedOperationException(String.format(
            "Unsupported CassandraColumnNameTranslator: %s for column: %s.",
            tableLayout.getDesc().getColumnNameTranslator(),
            tableLayout.getName()));
    }
  }

  /**
   * Translates a Cassandra column name to a Fiji column name.
   *
   * @param localityGroupTable The Cassandra table containing the column.
   * @param cassandraColumnName The Cassandra column name.
   * @return The Fiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract FijiColumnName toFijiColumnName(
      final CassandraTableName localityGroupTable,
      final CassandraColumnName cassandraColumnName
  ) throws NoSuchColumnException;

  /**
   * Translates a Fiji column name into a Cassandra column name.
   *
   * @param fijiColumnName The Fiji column name.
   * @return The Cassandra column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract CassandraColumnName toCassandraColumnName(
      final FijiColumnName fijiColumnName
  ) throws NoSuchColumnException;
}
