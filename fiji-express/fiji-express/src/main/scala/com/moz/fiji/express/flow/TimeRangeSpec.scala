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
import com.moz.fiji.schema.KConstants

/**
 * A specification of the range cell versions that should be read from a column in a Fiji table.
 *
 * @note Defaults to [[com.moz.fiji.express.flow.TimeRangeSpec.All TimeRangeSpec.All]].
 * @example TimeRangeSpec usage.
 *      - [[com.moz.fiji.express.flow.TimeRangeSpec.All TimeRangeSpec.All]] - Specify that all versions
 *        should be requested:
 *        {{{
 *          .withTimeRangeSpec(TimeRangeSpec.All)
 *        }}}
 *      - [[com.moz.fiji.express.flow.TimeRangeSpec.At TimeRangeSpec.At]] - Specify that a specific
 *        version should be requested:
 *        {{{
 *          // Gets only cells with the version 123456789.
 *          .withTimeRangeSpec(TimeRangeSpec.At(123456789L))
 *        }}}
 *      - [[com.moz.fiji.express.flow.TimeRangeSpec.From TimeRangeSpec.From]] - Specify that all
 *        versions after the specified version (inclusive) should be requested:
 *        {{{
 *          // Gets only cells with versions larger than 123456789.
 *          .withTimeRangeSpec(After(123456789L))
 *        }}}
 *      - [[com.moz.fiji.express.flow.TimeRangeSpec.Before TimeRangeSpec.Before]] - Specify that all
 *        versions before the specified version (exclusive) should be requested:
 *        {{{
 *          // Gets only cells with versions smaller than 123456789.
 *          .withTimeRangeSpec(Before(123456789L))
 *        }}}
 *      - [[com.moz.fiji.express.flow.TimeRangeSpec.Between TimeRangeSpec.Between]] - Specify that all
 *        versions between the two specified bounds should be requested:
 *        {{{
 *          // Gets only cells with versions between 12345678 and 123456789.
 *          .withTimeRangeSpec(Between(12345678L, 123456789L))
 *        }}}
 * @see [[com.moz.fiji.express.flow.FijiInput]] for more TimeRangeSpec usage information.
 */
@ApiAudience.Public
@ApiStability.Stable
sealed trait TimeRangeSpec extends Serializable {
  /** Earliest version of the TimeRange, inclusive. */
  def begin: Long

  /** Latest version of the TimeRange, exclusive. */
  def end: Long
}

/**
 * Provides [[com.moz.fiji.express.flow.TimeRangeSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object TimeRangeSpec {
  /**
   * Implementation of [[com.moz.fiji.express.flow.TimeRangeSpec]] for specifying that all versions
   * should be requested.
   *
   * @see [[com.moz.fiji.express.flow.TimeRangeSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object All extends TimeRangeSpec {
    override val begin: Long = KConstants.BEGINNING_OF_TIME
    override val end: Long = KConstants.END_OF_TIME
  }

  /**
   * Implementation of [[com.moz.fiji.express.flow.TimeRangeSpec]] for specifying that only the provided
   * version should be requested.
   *
   * @see [[com.moz.fiji.express.flow.TimeRangeSpec]] for more usage information.
   *
   * @param version to request.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class At(version: Long) extends TimeRangeSpec {
    override val begin: Long = version
    override val end: Long = version + 1L
  }

  /**
   * Specifies that all cell versions after the provided version should be requested (inclusive).
   *
   * @see [[com.moz.fiji.express.flow.TimeRangeSpec]] for more usage information.
   *
   * @param begin is the earliest version that should be requested (inclusive).
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class From(override val begin: Long) extends TimeRangeSpec {
    override val end: Long = KConstants.END_OF_TIME
  }

  /**
   * Specifies that all versions before the provided version should be requested (exclusive).
   *
   * @see [[com.moz.fiji.express.flow.TimeRangeSpec]] for more usage information.
   *
   * @param end is the latest version that should be requested (exclusive).
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Before(override val end: Long) extends TimeRangeSpec {
    override val begin: Long = KConstants.BEGINNING_OF_TIME
  }

  /**
   * Specifies that all versions between the provided begin and end versions should be requested.
   *
   * @see [[com.moz.fiji.express.flow.TimeRangeSpec]] for more usage information.
   *
   * @param begin is the earliest version that should be requested (inclusive).
   * @param end is the latest version that should be requested (exclusive).
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Between(
      override val begin: Long,
      override val end: Long
  ) extends TimeRangeSpec {
    // Ensure that the time range bounds are sensible.
    require(begin <= end, "Invalid time range specified: (%d, %d)".format(begin, end))
  }
}
