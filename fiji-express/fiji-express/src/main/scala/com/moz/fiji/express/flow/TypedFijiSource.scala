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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.moz.fiji.express.flow

import cascading.tap.Tap
import com.google.common.base.Objects
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mappable
import com.twitter.scalding.Mode
import com.twitter.scalding.Local
import com.twitter.scalding.Test
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import com.twitter.scalding.typed.TypedSink

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.express.flow.framework.FijiTap
import com.moz.fiji.express.flow.framework.LocalFijiTap
import com.moz.fiji.express.flow.framework.TypedFijiScheme
import com.moz.fiji.express.flow.framework.TypedLocalFijiScheme

/**
 * TypedFijiSource is a type safe representation of [[FijiSource]]. This class extends the
 * [[Mappable]] trait in scalding to allow compile time type checking.
 *
 * When reading from a Fiji table, a `TypedFijiSource` will provide a view of the FijiTable as a
 * collection of tuples that correspond to rows from the Fiji Table. The columns that need to be
 * read can be configured along with the time spans that cells retrieved must belong to.
 * Each retrieved row is wrapped in a [[ExpressResult]] object which contains methods to
 * allow access to the row data.
 *
 * When writing to a Fiji table, a `TypedFijiSource` expects the value in the TypedPipe to be
 * an Iterable of the type [[ExpressColumnOutput]]. An [[ExpressColumnOutput]] object holds
 * information required to determine the location of the data to be written in a Fiji table.
 *
 * End-users cannot directly obtain instances of `TypedFijiSource`. Instead,
 * they should use the factory methods provided as part of the [[com.moz.fiji.express.flow]] module.
 *
 * @param tableAddress Fiji URI addressing the Fiji table to read or write to.
 * @param timeRange The range that the versions of the cells read must belong to. Ignored when the
 *     source is used to write.
 * @param inputColumns A one-to-one mapping from field names to Fiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 * @param rowRangeSpec The specification for which interval of rows to scan.
 * @param rowFilterSpec The specification for which row filter to apply.
 * @param conv Tuple converter definition passed in implicitly.
 * @param tset Tuple setter definition passed in implicitly.
 * @tparam T The type of value being read or written via TypedFijiSource.
 */
@ApiAudience.Public
@ApiStability.Evolving
final class TypedFijiSource[T] (
    val tableAddress: String,
    val timeRange: TimeRangeSpec,
    val inputColumns: List[ColumnInputSpec] = List(),
    val rowRangeSpec: RowRangeSpec = RowRangeSpec.All,
    val rowFilterSpec: RowFilterSpec = RowFilterSpec.NoFilter
)(implicit conv: TupleConverter[ExpressResult], tset: TupleSetter[Iterable[ExpressColumnOutput[_]]])
extends Mappable[ExpressResult] with TypedSink[Iterable[ExpressColumnOutput[_]]] {

  private val uri: FijiURI = FijiURI.newBuilder(tableAddress).build()

  /**
   * Default implementation of a converter method that returns a [[TupleConverter]] for the super
    * type of [[ExpressResult]].
    *
    * @tparam U is the type parameter for the [[TupleConverter]] returned.
    * @return the [[TupleConverter]] object with the new type.
    */
  override def converter[U >: ExpressResult]: TupleConverter[U] =
      TupleConverter.asSuperConverter[ExpressResult, U](conv)

  /**
   * Default implementation of a converter method that returns a [[TupleSetter]] for the subtype of
   * type T
   * @tparam U is the type parameter for the [[TupleSetter]] returned.
   * @return the [[TupleSetter]] object with the new type.
   */
  override def setter[U <: Iterable[ExpressColumnOutput[_]]]: TupleSetter[U] =
      TupleSetter.asSubSetter[Iterable[ExpressColumnOutput[_]], U](tset)

  /** A Typed Fiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  val typedFijiScheme: TypedFijiScheme =
      new TypedFijiScheme(
          tableAddress,
          timeRange,
          inputColumns,
          rowRangeSpec,
          rowFilterSpec)

  /** A Typed Local Fiji scheme intended to be used with Scalding/Cascading's local mode. */
  val typedLocalFijiScheme: TypedLocalFijiScheme =
      new TypedLocalFijiScheme(
          uri,
          timeRange,
          inputColumns,
          rowRangeSpec,
          rowFilterSpec)

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[com.moz.fiji.schema.FijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Hdfs(_, _) => new FijiTap(uri, typedFijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalFijiTap(uri, typedLocalFijiScheme).asInstanceOf[Tap[_, _, _]]
      case Test(_) => new LocalFijiTap(uri, typedLocalFijiScheme).asInstanceOf[Tap[_, _, _]]
      case _ => throw new RuntimeException("Trying to create invalid tap")
    }
  }

  override def toString: String = {
    Objects
        .toStringHelper(this)
        .add("tableAddress", tableAddress)
        .add("timeRangeSpec", timeRange)
        .add("inputColumns", inputColumns)
        .add("rowRangeSpec", rowRangeSpec)
        .add("rowFilterSpec", rowFilterSpec)
        .toString
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: TypedFijiSource[T] => (
          tableAddress == other.tableAddress
              && inputColumns == other.inputColumns
              && timeRange == other.timeRange
              && rowRangeSpec == other.rowRangeSpec
              && rowFilterSpec == other.rowFilterSpec)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(
        tableAddress,
        inputColumns,
        timeRange,
        rowRangeSpec,
        rowFilterSpec)
  }
}
