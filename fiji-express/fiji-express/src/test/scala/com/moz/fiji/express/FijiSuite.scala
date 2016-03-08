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

package com.moz.fiji.express

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.HashMap

import org.scalatest.FunSuiteLike

import com.moz.fiji.express.flow.PagingSpec
import com.moz.fiji.express.flow.ExpressResult
import com.moz.fiji.express.flow.FlowCell
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.util.InstanceBuilder
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef


/** Contains convenience methods for writing tests that use Fiji. */
trait FijiSuite extends FunSuiteLike {
  // Counter for incrementing instance names by.
  val counter: AtomicInteger = new AtomicInteger(0)

  /**
   * Builds a slice containing no values.  This can be used to test for behavior of missing
   * values.
   *
   * @tparam T type of the values in the returned slice.
   * @return an empty slice.
   */
  def missing[T](): Seq[FlowCell[T]] = Seq()

  /**
   * Builds a slice from a group type column name and list of version, value pairs.
   *
   * @tparam T type of the values contained within desired slice.
   * @param columnName for a group type family, of the form "family:qualifier"
   * @param values pairs of (version, value) to build the slice with.
   * @return a slice containing the specified cells.
   */
  def slice[T](columnName: String, values: (Long, T)*): Seq[FlowCell[T]] = {
    val parsedName = new FijiColumnName(columnName)
    require(
        parsedName.isFullyQualified,
        "Fully qualified column names must be of the form \"family:qualifier\"."
    )
    values
        .map { entry: (Long, T) =>
          val (version, value) = entry
          FlowCell(parsedName.getFamily, parsedName.getQualifier, version, value)
        }
  }

  /**
   * Builds a list of ExpressResult containing the the values provided in a single column of the
   * specified row.
   *
   * @param table The instance of a fiji table.
   * @param entityId The entityId for the row.
   * @param columnName The name of the column.
   * @param values List of values that are to be contained in the returning ExpressResult.
   * @tparam T The type of the value provided.
   * @return A sequence of ExpressResult's containing the supplied values.
   */
  def fijiRowDataSlice[T](
      table: FijiTable,
      entityId: String,
      columnName: String,
      values: (Long, T)*
  ): List[ExpressResult] = {

    val parsedName = FijiColumnName.create(columnName)
    val entity = EntityIdFactory.getFactory(table.getLayout).getEntityId(entityId)
    ResourceUtil.doAndClose(table.getWriterFactory.openAtomicPutter) {
      atomicPutter =>
        atomicPutter.begin(entity)
        values.foreach {
          value: (Long, T) =>
            val (timestamp, valString) = value
            atomicPutter.put(parsedName.getFamily, parsedName.getQualifier, timestamp, valString)
        }
        atomicPutter.commit()
    }

    val dummyDataRequest: FijiDataRequest =
      FijiDataRequest.builder()
        .addColumns(
          ColumnsDef.create().withMaxVersions(5)
            .add(parsedName.getFamily, parsedName.getQualifier))
        .build()
    val retList: List[ExpressResult] = List(
        ResourceUtil.doAndClose(table.getReaderFactory.openTableReader) {
          reader =>
            ExpressResult(
                reader.get(entity, dummyDataRequest),
                HashMap[FijiColumnName, PagingSpec]())
        })
    retList.toList
  }

  /**
   * Builds a slice from a map type column name and a list of qualifier, version, value triples.
   *
   * @tparam T type of the values contained within desired slice.
   * @param columnName for a map type family, of the form "family"
   * @param values are triples of (qualifier, version, value) to build the slice with.
   * @return a slice containing the specified cells.
   */
  def mapSlice[T](columnName: String, values: (String, Long, T)*): Seq[FlowCell[T]] = {
    val parsedName = new FijiColumnName(columnName)
    require(
        !parsedName.isFullyQualified,
        "Column family names must not contain any ':' characters."
    )

    values
        .map { entry: (String, Long, T) =>
          val (qualifier, version, value) = entry
          FlowCell(parsedName.getFamily, qualifier, version, value)
        }
  }

  /**
   * Constructs and starts a test Fiji instance that uses fake-hbase.
   *
   * @param instanceName Name of the test Fiji instance.
   * @return A handle to the Fiji instance that just got constructed. Note: This object must be
   *     {{{release()}}}'d once it is no longer needed.
   */
  def makeTestFiji(instanceName: String = "default"): Fiji = {
    new InstanceBuilder(instanceName).build()
  }

  /**
   * Constructs and starts a test Fiji instance and creates a Fiji table.
   *
   * @param layout Layout of the test table.
   * @param instanceName Name of the Fiji instance to create.
   * @return A handle to the Fiji table that just got constructed. Note: This object must be
   *     {{{release()}}}'d once it is no longer needed.
   */
  def makeTestFijiTable(
      layout: FijiTableLayout,
      instanceName: String = "default_%s".format(counter.incrementAndGet())
  ): FijiTable = {
    val tableName = layout.getName
    val fiji: Fiji = new InstanceBuilder(instanceName)
        .withTable(tableName, layout)
        .build()

    val table: FijiTable = fiji.openTable(tableName)
    fiji.release()
    return table
  }
}
