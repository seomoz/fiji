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
package com.moz.fiji.express.flow.framework

import java.io.InputStream
import java.io.OutputStream
import java.util.Properties

import scala.collection.immutable.HashMap

import cascading.flow.FlowProcess
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tuple.Tuple
import cascading.flow.hadoop.util.HadoopUtil
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.express.flow.ExpressColumnOutput
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.PagingSpec
import com.moz.fiji.express.flow.TimeRangeSpec
import com.moz.fiji.express.flow.util.ResourceUtil.withFijiTable

/**
 * A local version of [[com.moz.fiji.express.flow.framework.TypedFijiScheme]] that is meant to be used
 * with Cascading's local job runner.
 *
 * [[TypedLocalFijiScheme]] extends trait [[com.moz.fiji.express.flow.framework.BaseLocalFijiScheme]]
 * which holds method implementations of [[cascading.scheme.Scheme]] that are common to both
 * [[LocalFijiScheme]] and [[TypedLocalFijiScheme]] for running jobs locally.
 *
 * This scheme is meant to be used with [[com.moz.fiji.express.flow.framework.LocalFijiTap]] and
 * Cascading's local job runner. Jobs run with Cascading's local job runner execute on
 * your local machine instead of a cluster. This can be helpful for testing or quick jobs.
 *
 * This scheme is responsible for converting rows from a Fiji table that are input to a Cascading
 * flow into Cascading tuples
 * (see* `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`)
 * and writing output data from a Cascading flow to a Fiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * @see[[com.moz.fiji.express.flow.framework.TypedFijiScheme]]
 * @see[[com.moz.fiji.express.flow.framework.BaseLocalFijiScheme]]
 *
 * @param uri The URI identifying the target fiji table.
 * @param timeRange The time range specifying the versions to use from the fiji table.
 *     Use None if all values should be written at the current time.
 * @param inputColumns The list of columns to be read as input.
 * @param rowRangeSpec  The specifications for the range of the input.
 * @param rowFilterSpec The specifications for the the filters on the input.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
private[express] case class TypedLocalFijiScheme(
    private[express] val uri: FijiURI,
    private[express] val timeRange: TimeRangeSpec,
    private[express] val inputColumns: List[ColumnInputSpec] = List(),
    private[express] val rowRangeSpec: RowRangeSpec,
    private[express] val rowFilterSpec: RowFilterSpec
) extends BaseLocalFijiScheme {

  // Map of columns that were requested paged.
  private val pagedColumnMap : Map[FijiColumnName, PagingSpec] =
      HashMap(
          inputColumns
              .filterNot { col: ColumnInputSpec => PagingSpec.Off.equals(col.pagingSpec)}
              .map { col: ColumnInputSpec => col.columnName -> col.pagingSpec}:_*)

  /**
   * Reads and converts a row from a Fiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process The flow process currently being run.
   * @param sourceCall The source call that contains the context for this source.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *         <code>false</code> if there were no more rows to read.
   */
  override def source(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]
  ): Boolean = {
    val context: InputContext = sourceCall.getContext
    if (context.iterator.hasNext) {
      // Get the current row.
      val row: FijiRowData = context.iterator.next()
      val result: Tuple = TypedFijiScheme.rowToTuple(row, pagedColumnMap)

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      process.increment(BaseFijiScheme.CounterGroupName, BaseFijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
    }
  }

  /**
   * Sets up any resources required to read from a Fiji table.
   *
   * @param process The flow process currently being run.
   * @param sourceCall The source call that contains the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]
  ) {

    val jobConfiguration: JobConf =
        HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))

    withFijiTable(uri, jobConfiguration) { table: FijiTable =>
    // Build the input context.
      val request = BaseFijiScheme.buildRequest(table.getLayout, timeRange, inputColumns)
      val context: InputContext = BaseLocalFijiScheme.configureInputContext(
          table,
          jobConfiguration,
          rowFilterSpec,
          rowRangeSpec,
          request)

      sourceCall.setContext(context)
    }
  }

  /**
   * Sets up any resources required to write to a Fiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkPrepare(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputStream]
  ) {
    val conf: JobConf =
        HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))
    withFijiTable(uri, conf) { table =>
      // Set the sink context to an opened FijiTableWriter.
      sinkCall.setContext(
          DirectFijiSinkContext(
              EntityIdFactory.getFactory(table.getLayout),
              table.getWriterFactory.openBufferedWriter()))
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Fiji table. The input type to the sink is expected
   * to be either a [[com.moz.fiji.express.flow.ExpressColumnOutput]] object, or a Tuple of multiple
   * ExpressColumnOutput objects where each element in the tuple corresponds to a column of
   * the Fiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sink(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputStream]
  ) {

    val DirectFijiSinkContext(eidFactory, writer) = sinkCall.getContext
    def writeSingleVal(singleVal:ExpressColumnOutput[_]) :Unit= {
      singleVal.version match {
        case Some(timestamp) =>
          writer.put(
              singleVal.entityId.toJavaEntityId (eidFactory),
              singleVal.family,
              singleVal.qualifier,
              timestamp,
              singleVal.encode (singleVal.datum))
        case None =>
          writer.put(
              singleVal.entityId.toJavaEntityId (eidFactory),
              singleVal.family,
              singleVal.qualifier,
              HConstants.LATEST_TIMESTAMP,
              singleVal.encode (singleVal.datum))
      }
    }
    //The first object in tuple entry contains the data in the pipe.
    sinkCall.getOutgoingEntry.getObject(0) match {
      //Value being written to multiple columns.
      case nColumnOutput: Iterable[ExpressColumnOutput[_]] =>
        nColumnOutput.foreach { anyVal: ExpressColumnOutput[_] =>
          writeSingleVal(anyVal)
        }
      case _ => throw new RuntimeException("Incorrect type. " +
          "The typed sink expects a type Iterable[ExpressColumnOutput[_]]")
    }
  }
}
