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

package com.moz.fiji.express.flow.framework.hfile

import cascading.tap.Tap
import com.twitter.scalding.AccessMode
import com.twitter.scalding.CascadingLocal
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Test
import com.twitter.scalding.TestMode
import com.twitter.scalding.TestTapFactory
import com.twitter.scalding.Write

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.ColumnOutputSpec

/**
 * A read or write view of a Fiji table.
 *
 * A Scalding `Source` provides a view of a data source that can be read as Scalding tuples. It
 * is comprised of a Cascading tap [[cascading.tap.Tap]], which describes where the data is and how
 * to access it, and a Cascading Scheme [[cascading.scheme.Scheme]], which describes how to read
 * and interpret the data.
 *
 * An `HFileFijiSource` should never be used for reading.  It is intended to be used for writing
 * to HFiles formatted for bulk-loading into Fiji.
 *
 * When writing to a Fiji table, a `HFileFijiSource` views a Fiji table as a collection of tuples
 * that correspond to cells from the Fiji table. Each tuple to be written must provide a cell
 * address by specifying a Fiji `EntityID` in the tuple field `entityId`, a value to be written in a
 * configurable field, and (optionally) a timestamp in a configurable field.
 *
 * End-users cannot directly obtain instances of `FijiSource`. Instead,
 * they should use the factory methods provided as part of the
 * [[com.moz.fiji.express.flow.framework.hfile]] module.
 *
 * @param tableAddress is a Fiji URI addressing the Fiji table to read or write to.
 * @param timestampField is the name of a tuple field that will contain cell timestamp when the
 *     source is used for writing. Specify `None` to write all cells at the current time.
 * @param columns is a one-to-one mapping from field names to Fiji columns. When reading,
 *     the columns in the map will be read into their associated tuple fields. When
 *     writing, values from the tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Stable
final case class HFileFijiSource private[express] (
    tableAddress: String,
    hFileOutput: String,
    timestampField: Option[Symbol],
    columns: Map[Symbol, ColumnOutputSpec]
) extends Source {
  import com.moz.fiji.express.flow.FijiSource._

  private val hfileScheme = new HFileFijiScheme(timestampField, convertKeysToStrings(columns))

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[com.moz.fiji.schema.FijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    (readOrWrite, mode) match {
      case (Write, Hdfs(_, _)) =>  new HFileFijiTap(tableAddress, hfileScheme, hFileOutput)
      case (Write, mode: Test) =>
        TestTapFactory(this, hfileScheme.getSinkFields).createTap(readOrWrite)(mode)
      case (Write, mode: HadoopTest) =>
        TestTapFactory(this, hfileScheme.getSinkFields).createTap(readOrWrite)(mode)
      case (Write, Local(_)) =>
        throw new UnsupportedOperationException("Cascading local mode unsupported")
      case (Read, _) => throw new UnsupportedOperationException("Read unsupported")
      case (accessMode: AccessMode, mode: Mode) =>
        throw new UnsupportedOperationException(
            "Unable to handle AccessMode %s and Mode %s.".format(accessMode, mode)
        )
    }
  }
}
