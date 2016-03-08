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

import scala.collection.JavaConverters.asScalaIteratorConverter

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.util.AvroUtil
import com.moz.fiji.express.flow.framework.BaseFijiScheme
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.ColumnVersionIterator
import com.moz.fiji.schema.MapFamilyVersionIterator

/**
 * A wrapper class around [[com.moz.fiji.schema.FijiRowData]] that contains methods to retrieve and
 * iterate through the data. This class is used as the type for
 * [[com.twitter.scalding.typed.TypedPipe]] received from [[com.moz.fiji.express.flow.TypedFijiSource]]
 * for the type safe API.
 *
 * @param row of the requested data from the Fiji table.
 * @param pagedColumnMap The map of input columns that are requested paged.
 */
@ApiAudience.Public
@ApiStability.Evolving
final class ExpressResult private(
    row: FijiRowData,
    pagedColumnMap: Map[FijiColumnName, PagingSpec]
) {
  /**
   * Fetch the [[EntityId]] for the row.
   *
   * @return the entityId for the row.
   */
  def entityId: EntityId = EntityId.fromJavaEntityId(row.getEntityId)

  /**
   * Fetch the most recent cell for a  qualified column.
   *
   * @tparam T is the type of the datum contained in [[FlowCell]].
   * @return the [[FlowCell]] containing the most recent cell.
   */
  def mostRecentCell[T](
      family: String,
      qualifier: String
  ): FlowCell[T] = {
    FlowCell(row.getMostRecentCell(family, qualifier))
  }

  /**
   * Fetch a cell with a specific timestamp.
   *
   * @param timestamp is timestamp associated with the requested cell.
   * @tparam T is the type of the datum contained in [[FlowCell]]
   * @return a [[FlowCell]] that contains the requested cell.
   */
  def cell[T](
      family: String,
      qualifier: String,
      timestamp: Long
  ): FlowCell[T] = {
    FlowCell(row.getCell(family, qualifier, timestamp))
  }

  /**
   * Fetches a sequence of cells for the requested column.
   *
   * @tparam T is the type of the datum that will be contained in [[FlowCell]]
   * @return an iterator of [[FlowCell]] for the column requested.
   */
  def qualifiedColumnCells[T](
      family: String,
      qualifier: String
  ): Seq[FlowCell[T]] = {
    pagedColumnMap.get(FijiColumnName.create(family, qualifier)) match {
      case Some(PagingSpec.Cells(pageSize)) =>
        def genItr(): Iterator[FlowCell[T]] = {
          new ColumnVersionIterator(row, family, qualifier, pageSize)
            .asScala
            .map { entry: java.util.Map.Entry[java.lang.Long, _] =>
              FlowCell(
                  family,
                  qualifier,
                  entry.getKey,
                  AvroUtil.avroToScala(entry.getValue).asInstanceOf[T]
              )
            }
        }
        new TransientStream[FlowCell[T]](genItr)
      // Column is not paged.
      case _ =>
        row
          .iterator[T](family, qualifier)
          .asScala
          .toList
          .map { fijiCell: FijiCell[T] => FlowCell(fijiCell)}
      }
    }

  /**
   * Fetches a sequence of cells for the requested family. For a map type family the type T should
   * represent the expected datum type. However, for group type family the type will have to be
   * specified as "Any" or "_", since the returning datum types can vary.
   *
   * @param family of the column requested.
   * @tparam T is the type of the datum that will be contained in [[FlowCell]]
   * @return an iterator of [[FlowCell]]'s for the family requested.
   */
  def columnFamilyCells[T](
      family: String
  ): Seq[FlowCell[T]] = {
    pagedColumnMap.get(FijiColumnName.create(family)) match {
      case Some(PagingSpec.Cells(pageSize)) =>
        def genItr(): Iterator[FlowCell[T]] = {
          new MapFamilyVersionIterator(row, family, BaseFijiScheme.qualifierPageSize, pageSize)
              .asScala
              .map { entry: MapFamilyVersionIterator.Entry[_] =>
                FlowCell(
                    family,
                    entry.getQualifier,
                    entry.getTimestamp,
                    AvroUtil.avroToScala(entry.getValue).asInstanceOf[T])
              }
        }
        new TransientStream[FlowCell[T]](genItr)
      // Column is not paged.
      case _ =>
        row.iterator[T](family)
            .asScala
            .toList
            .map { fijiCell: FijiCell[T] => FlowCell(fijiCell) }
    }
  }
}

/**
 * Companion object for the ExpressResult class.
 */
object ExpressResult {
  /**
   * Creates and returns an instance of ExpressResult.
   *
   * @param rowData A row of requested data from Fiji Table.
   * @param pagedColumnMap The map of input columns that are requested with paging.
   * @return an instance of ExpressResult for the specified row.
   */
  def apply(
      rowData: FijiRowData,
      pagedColumnMap: Map[FijiColumnName, PagingSpec]
  ): ExpressResult = new ExpressResult(rowData, pagedColumnMap)
}
