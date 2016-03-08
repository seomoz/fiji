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
import com.moz.fiji.schema.filter.FijiColumnFilter
import com.moz.fiji.schema.filter.FijiColumnRangeFilter
import com.moz.fiji.schema.filter.RegexQualifierColumnFilter

// This override exists to prevent issues with line breaks in scaladoc links.
// scalastyle:off line.size.limit
/**
 * A specification describing a column filter to use when reading data from a Fiji table. Column
 * filters select which columns should be read from the Fiji table.
 *
 * Filters are implemented via HBase filters and execute on a cluster so they can cut down on the
 * amount of data transferred over the network. Filters can be combined with:
 *  - [[com.moz.fiji.express.flow.ColumnFilterSpec.And]]
 *  - [[com.moz.fiji.express.flow.ColumnFilterSpec.Or]]
 * which are themselves filters.
 *
 * @note Defaults to [[com.moz.fiji.express.flow.ColumnFilterSpec.NoFilter ColumnFilterSpec.NoFilter]].
 * @example ColumnFilterSpec usage.
 *      - [[com.moz.fiji.express.flow.ColumnFilterSpec.NoFilter ColumnFilterSpec.NoFilter]] - Reading
 *        data using no column filters:
 *        {{{
 *          .withFilterSpec(ColumnFilterSpec.NoFilter)
 *        }}}
 *      - [[com.moz.fiji.express.flow.ColumnFilterSpec.ColumnRange ColumnFilterSpec.ColumnRange]] -
 *        Reading data from columns within the provided column qualifier range:
 *        {{{
 *          // Filters out columns with names not between "c" (inclusive) and "m" (exclusive).
 *          .withFilterSpec(
 *              ColumnFilterSpec.ColumnRange(
 *                  minimum = "c",
 *                  maximum = "m",
 *                  minimumIncluded = true,
 *                  maximumIncluded = false
 *              )
 *          )
 *        }}}
 *      - [[com.moz.fiji.express.flow.ColumnFilterSpec.Regex ColumnFilterSpec.Regex]] - Reading data
 *        from columns with names matching the provided regular expression:
 *        {{{
 *          // Filters out columns with names that are not alphanumeric.
 *          .withFilterSpec(ColumnFilterSpec.Regex("[a-zA-Z0-9]*"))
 *        }}}
 *      - [[com.moz.fiji.express.flow.ColumnFilterSpec.FijiSchemaColumnFilter ColumnFilterSpec.FijiMRColumnFilter]] -
 *        Reading data from columns that match the provided Fiji MR column filter:
 *        {{{
 *          val filter: FijiColumnFilter = // ...
 *
 *          // ...
 *
 *          // Filters out columns that don't match the provided Fiji MR column filter.
 *          .withFilterSpec(ColumnFilterSpec.FijiMRColumnFilter(filter))
 *        }}}
 *      - [[com.moz.fiji.express.flow.ColumnFilterSpec.And ColumnFilterSpec.And]] - Reading data from
 *        columns matching the provided list of column filters:
 *        {{{
 *          val filters: Seq[ColumnFilterSpec] = Seq(
 *              ColumnFilterSpec.ColumnRange("California", "Maine"),
 *              ColumnFilterSpec.Regex("[^ ]*")
 *          )
 *
 *          // Selects columns with single word names between "California" and "Maine".
 *          .withFilterSpec(ColumnFilterSpec.And(filters))
 *        }}}
 *      - [[com.moz.fiji.express.flow.ColumnFilterSpec.Or ColumnFilterSpec.Or]] - Reading data from
 *        columns matching the provided list of column filters:
 *        {{{
 *          val filters: Seq[ColumnFilterSpec] = Seq(
 *              ColumnFilterSpec.ColumnRange("California", "Maine"),
 *              ColumnFilterSpec.Regex("[0-9]*")
 *          )
 *
 *          // Selects columns with a numeric name or a name between "California" and "Maine".
 *          .withFilterSpec(ColumnFilterSpec.Or(filters))
 *        }}}
 * @see [[com.moz.fiji.express.flow.ColumnInputSpec]] for more ColumnFilterSpec usage information.
 */
// scalastyle:on line.size.limit
@ApiAudience.Public
@ApiStability.Stable
sealed trait ColumnFilterSpec {
  /** @return a FijiColumnFilter that corresponds to the Express column filter. */
  private[fiji] def toFijiColumnFilter: Option[FijiColumnFilter]
}

/**
 * Provides [[com.moz.fiji.express.flow.ColumnFilterSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object ColumnFilterSpec {
  /**
   * Specifies that columns should be filtered out using a list of column filters combined using a
   * logical "AND" operator. Only columns that pass all of the provided filters will be read.
   *
   * @see [[com.moz.fiji.express.flow.ColumnFilterSpec]] for more usage information.
   *
   * @param filters to combine with a logical "AND" operation.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class And(filters: Seq[ColumnFilterSpec])
      extends ColumnFilterSpec {
    private[fiji] override def toFijiColumnFilter: Option[FijiColumnFilter] = {
      val schemaFilters = filters
          .map { filter: ColumnFilterSpec => filter.toFijiColumnFilter.get }
          .toArray
      Some(Filters.and(schemaFilters: _*))
    }
  }

  /**
   * Specifies that columns should be filtered out using a list of column filters combined using a
   * logical "OR" operator. Only columns that pass one or more of the provided filters will be read.
   *
   * @see [[com.moz.fiji.express.flow.ColumnFilterSpec]] for more usage information.
   *
   * @param filters to combine with a logical "OR" operation.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Or(filters: Seq[ColumnFilterSpec])
      extends ColumnFilterSpec {
    private[fiji] override def toFijiColumnFilter: Option[FijiColumnFilter] = {
      val orParams = filters
          .map { filter: ColumnFilterSpec => filter.toFijiColumnFilter.get }
          .toArray
      Some(Filters.or(orParams: _*))
    }
  }

  /**
   * Specifies that columns with names in between the specified bounds should be selected.
   *
   * @see [[com.moz.fiji.express.flow.ColumnFilterSpec]] for more usage information.
   *
   * @param minimum qualifier bound.
   * @param maximum qualifier bound.
   * @param minimumIncluded determines if the lower bound is inclusive.
   * @param maximumIncluded determines if the upper bound is inclusive.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class ColumnRange(
      minimum: Option[String] = None,
      maximum: Option[String] = None,
      minimumIncluded: Boolean = true,
      maximumIncluded: Boolean = false
  ) extends ColumnFilterSpec {
    private[fiji] override def toFijiColumnFilter: Option[FijiColumnFilter] = {
      Some(new FijiColumnRangeFilter(
          minimum.getOrElse { null },
          minimumIncluded,
          maximum.getOrElse { null },
          maximumIncluded))
    }
  }

  /**
   * Specifies that columns with names matching the provided regular expression should be selected.
   *
   * @see [[com.moz.fiji.express.flow.ColumnFilterSpec]] for more usage information.
   *
   * @param regex to match on.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Regex(regex: String)
      extends ColumnFilterSpec {
    private[fiji] override def toFijiColumnFilter: Option[FijiColumnFilter] =
        Some(new RegexQualifierColumnFilter(regex))
  }

  /**
   * Specifies that columns should be filtered out using the underlying FijiColumnFilter.
   *
   * @see [[com.moz.fiji.express.flow.ColumnFilterSpec]] for more usage information.
   *
   * @param fijiColumnFilter specifying the filter conditions.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class FijiSchemaColumnFilter(fijiColumnFilter: FijiColumnFilter)
      extends ColumnFilterSpec {
    private[fiji] override def toFijiColumnFilter: Option[FijiColumnFilter] = Some(fijiColumnFilter)
  }

  /**
   * Specifies that no column filters should be used.
   *
   * @see [[com.moz.fiji.express.flow.ColumnFilterSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object NoFilter
      extends ColumnFilterSpec {
    private[fiji] override def toFijiColumnFilter: Option[FijiColumnFilter] = None
  }
}
