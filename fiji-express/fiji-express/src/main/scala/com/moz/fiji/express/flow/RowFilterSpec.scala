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

import com.moz.fiji.schema.filter.Filters
import com.moz.fiji.schema.filter.FijiRandomRowFilter
import com.moz.fiji.schema.filter.FijiRowFilter

/**
 * A specification describing a row filter to use when reading data from a Fiji table.
 *
 * Filters are implemented via HBase filters and execute on a cluster so they can cut down on the
 * amount of data transferred over the network. Filters can be combined with:
 *  - [[com.moz.fiji.express.flow.RowFilterSpec.And]]
 *  - [[com.moz.fiji.express.flow.RowFilterSpec.Or]]
 * which are themselves filters.
 *
 * @note Defaults to [[com.moz.fiji.express.flow.RowFilterSpec.NoFilter RowFilterSpec.NoFilter]].
 * @example RowFilterSpec usage.
 *      - [[com.moz.fiji.express.flow.RowFilterSpec.NoFilter RowFilterSpec.NoFilter]] - Reading rows
 *        without a filter:
 *        {{{
 *          .withRowFilterSpec(RowFilterSpec.NoFilter)
 *        }}}
 *      - [[com.moz.fiji.express.flow.RowFilterSpec.Random RowFilterSpec.Random]] - Reading rows that
 *        pass a random chance filter:
 *        {{{
 *          // Select 20% of rows.
 *          .withRowFilterSpec(RowFilterSpec.Random(0.2))
 *        }}}
 *      - [[com.moz.fiji.express.flow.RowFilterSpec.FijiSchemaRowFilter RowFilterSpec.FijiMRRowFilter]]
 *        - Reading rows that pass an underlying Fiji MR row filter:
 *        {{{
 *          val underlyingFilter: FijiRowFilter = // ...
 *
 *          // ...
 *
 *          // Select rows using a Fiji MR row filter.
 *          .withRowFilterSpec(RowFilterSpec.FijiMRRowFilter(underlyingFilter))
 *        }}}
 *      - [[com.moz.fiji.express.flow.RowFilterSpec.And RowFilterSpec.And]] - Reading rows that pass a
 *        list of filters:
 *        {{{
 *          val filters: Seq[RowFilterSpec] = Seq(
 *              RowFilterSpec.Random(0.2),
 *              RowFilterSpec.Random(0.2)
 *          )
 *
 *          // Select rows that pass two 20% chance random filters.
 *          .withRowFilterSpec(RowFilterSpec.And(filters))
 *        }}}
 *      - [[com.moz.fiji.express.flow.RowFilterSpec.Or RowFilterSpec.Or]] - Reading rows that any of
 *        the filters in a list:
 *        {{{
 *          val filters: Seq[RowFilterSpec] = Seq(
 *              RowFilterSpec.Random(0.2),
 *              RowFilterSpec.Random(0.2)
 *          )
 *
 *          // Select rows pass any of the provided filters.
 *          .withRowFilterSpec(RowFilterSpec.Or(filters))
 *        }}}
 * @see [[com.moz.fiji.express.flow.FijiInput]] for more RowFilterSpec usage information.
 */
@ApiAudience.Public
@ApiStability.Stable
sealed trait RowFilterSpec extends Serializable {
  private[fiji] def toFijiRowFilter: Option[FijiRowFilter]
}

/**
 * Provides [[com.moz.fiji.express.flow.RowFilterSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object RowFilterSpec {
  /**
   * Specifies that rows should be filtered out using a list of row filters combined using a logical
   * "AND" operator. Only rows that pass all of the provided filters will be read.
   *
   * @see [[com.moz.fiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param filters to combine with a logical "AND" operation.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class And(filters: Seq[RowFilterSpec])
      extends RowFilterSpec {
    private[fiji] override def toFijiRowFilter: Option[FijiRowFilter] = {
      val schemaFilters = filters
          .map { filter: RowFilterSpec => filter.toFijiRowFilter.get }
          .toArray
      Some(Filters.and(schemaFilters: _*))
    }
  }

  /**
   * Specifies that rows should be filtered out using a list of row filters combined using a logical
   * "OR" operator. Only rows that pass one or more of the provided filters will be read.
   *
   * @see [[com.moz.fiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param filters to combine with a logical "OR" operation.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Or(filters: Seq[RowFilterSpec])
      extends RowFilterSpec {
    private[fiji] override def toFijiRowFilter: Option[FijiRowFilter] = {
      val orParams = filters
          .map { filter: RowFilterSpec => filter.toFijiRowFilter.get }
          .toArray
      Some(Filters.or(orParams: _*))
    }
  }

  /**
   * Specifies that rows should be filtered out randomly with a user-provided chance. Chance
   * represents the probability that a row will be selected.
   *
   * @see [[com.moz.fiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param chance by which to select a row. Should be between 0.0 and 1.0.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Random(chance: Float)
      extends RowFilterSpec {
    private[fiji] override def toFijiRowFilter: Option[FijiRowFilter] =
        Some(new FijiRandomRowFilter(chance))
  }

  /**
   * Specifies that rows should be filtered out using the underlying FijiRowFilter.
   *
   * @see [[com.moz.fiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param fijiRowFilter specifying the filter conditions.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class FijiSchemaRowFilter(fijiRowFilter: FijiRowFilter)
      extends RowFilterSpec {
    private[fiji] override def toFijiRowFilter: Option[FijiRowFilter] = Some(fijiRowFilter)
  }

  /**
   * Specifies that no row filters should be used.
   *
   * @see [[com.moz.fiji.express.flow.RowFilterSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object NoFilter
      extends RowFilterSpec {
    private[fiji] override def toFijiRowFilter: Option[FijiRowFilter] = None
  }
}
