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

package com.moz.fiji.express.flow

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability

/**
 * A specification of the type of paging to use.
 *
 * If paging is disabled, all cells from the specified column will be loaded into memory at once. If
 * the size of the loaded cells exceeds the capacity of the task machine's main memory, the Scalding
 * job will fail at runtime with an [[java.lang.OutOfMemoryError]].
 *
 * @note Defaults to [[com.moz.fiji.express.flow.PagingSpec.Off PagingSpec.Off]].
 * @example PagingSpec usage.
 *      - [[com.moz.fiji.express.flow.PagingSpec.Cells PagingSpec.Cells]] - Paging by number of cells:
 *        {{{
 *          // Will read in 10 cells from Fiji at a time. The appropriate number of cells to be
 *          // paged in depends on the size of each cell. Users should try to retrieve as many cells
 *          // as possible without causing an OutOfMemoryError in order to increase performance.
 *          .withPagingSpec(PagingSpec.Cells(10))
 *        }}}
 *      - [[com.moz.fiji.express.flow.PagingSpec.Off PagingSpec.Off]] - No paging:
 *        {{{
 *          // Will disable paging entirely.
 *          .withPagingSpec(PagingSpec.Off)
 *        }}}
 * @see [[com.moz.fiji.express.flow.ColumnInputSpec]] for more PagingSpec usage information.
 */
@ApiAudience.Public
@ApiStability.Stable
sealed trait PagingSpec extends Serializable {
  private[fiji] def cellsPerPage: Option[Int]
}

/**
 * Provides [[com.moz.fiji.express.flow.PagingSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object PagingSpec {
  /**
   * Specifies that paging should not be used. Each row requested from Fiji tables will be fully
   * materialized into RAM.
   *
   * @see [[com.moz.fiji.express.flow.PagingSpec]] for more PagingSpec usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object Off extends PagingSpec {
    override val cellsPerPage: Option[Int] = None
  }

  /**
   * Specifies that paging should be enabled. Each page will contain the specified number of cells.
   *
   * @note Cells may not all be the same size (in bytes).
   * @see [[com.moz.fiji.express.flow.PagingSpec]] for more PagingSpec usage information.
   *
   * @param count of the cells per page.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Cells(count: Int) extends PagingSpec {
    override val cellsPerPage: Option[Int] = Some(count)
  }
}
