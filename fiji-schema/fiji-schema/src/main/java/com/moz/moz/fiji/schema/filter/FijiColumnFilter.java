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
import java.io.Serializable;

import org.apache.hadoop.hbase.filter.Filter;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;

/**
 * A column filter provides a means of filtering cells from a column on the server side.
 *
 * <p>To make your jobs more efficient, you may use a FijiColumnFilter to specify that
 * certain cells from a column be filtered. The cells will be filtered on the server,
 * which reduces the amount of data that needs to be sent to the client.</p>
 *
 * <p>FijiColumnFilters filter cells from a column, in contrast with FijiRowFilters, which
 * filters rows from a table.</p>
 *
 * <p>Classes extending FijiColumnFilter must implement the <code>hashCode</code> and
 * <code>equals</code> methods.</p>
 *
 * @see com.moz.fiji.schema.filter.FijiRowFilter
 * @see com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef#withFilter(FijiColumnFilter)
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
public abstract class FijiColumnFilter implements Serializable {
  /**
   * An object available to FijiColumnFilters that can be used to help implement the
   * toHBaseFilter() method.
   */
  @ApiAudience.Public
  @Inheritance.Sealed
  public abstract static class Context {
    /**
     * Converts a Fiji column name to an HBase column name.
     *
     * @param fijiColumnName The name of a fiji column.
     * @return The name of the HBase column that stores the fiji column data.
     * @throws NoSuchColumnException If there is no such column in the fiji table.
     */
    public abstract HBaseColumnName getHBaseColumnName(FijiColumnName fijiColumnName)
        throws NoSuchColumnException;
  }

  /**
   * Expresses the FijiColumnFilter in terms an equivalent HBase Filter.
   *
   * @param fijiColumnName The column this filter applies to.
   * @param context The context.
   * @return An equivalent HBase Filter.
   * @throws IOException If there is an error.
   */
  public abstract Filter toHBaseFilter(FijiColumnName fijiColumnName, Context context)
      throws IOException;
}
