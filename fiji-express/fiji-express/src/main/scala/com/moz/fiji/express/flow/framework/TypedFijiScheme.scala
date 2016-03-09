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

import scala.collection.immutable.HashMap

import cascading.scheme.SourceCall
import cascading.scheme.SinkCall
import cascading.flow.FlowProcess
import cascading.tap.Tap
import cascading.tuple.Tuple
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.schema.{EntityId => JEntityId}
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.ExpressResult
import com.moz.fiji.express.flow.ExpressColumnOutput
import com.moz.fiji.express.flow.TimeRangeSpec
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.PagingSpec
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.express.flow.framework.serialization.FijiKryoExternalizer

/**
 * A Fiji-specific implementation of a Cascading `Scheme` for the scalding type safe API, that
 * defines how to read and write the data in a Fiji table.
 *
 * [[TypedFijiScheme]] extends trait [[com.moz.fiji.express.flow.framework.BaseFijiScheme]], which holds
 * method implementations of [[cascading.scheme.Scheme]] that are common to both [[FijiScheme]]
 * and [[TypedFijiScheme]] for running mapreduce jobs.
 *
 * [[TypedFijiScheme]] is responsible for converting rows from a Fiji table that are input into a
 * Cascading flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Fiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * [[TypedFijiScheme]] must be used with [[com.moz.fiji.express.flow.framework.FijiTap]], since it
 * expects the Tap to have access to a Fiji table. [[com.moz.fiji.express.flow.TypedFijiSource]] handles
 * the creation of both [[TypedFijiScheme]] and [[FijiTap]] in FijiExpress.
 *
 * @see[[com.moz.fiji.express.flow.framework.BaseFijiScheme]]
 *
 * @param tableAddress The URI identifying of the target Fiji table.
 * @param timeRange The range of versions to include from the Fiji table.
 * @param icolumns A list of ColumnInputSpecs from where the data is to be read.
 * @param rowRangeSpec  The specifications for the range of the input.
 * @param rowFilterSpec The specifications for the the filters on the input.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
class TypedFijiScheme private[express] (
    private[express] val tableAddress: String,
    private[express] val timeRange: TimeRangeSpec,
    icolumns: List[ColumnInputSpec] = List(),
    private[express] val rowRangeSpec: RowRangeSpec,
    private[express] val rowFilterSpec: RowFilterSpec
) extends BaseFijiScheme {
  import TypedFijiScheme._

  private def uri: FijiURI = FijiURI.newBuilder(tableAddress).build()

  private[this] val mInputColumns = FijiKryoExternalizer(icolumns)

  def inputColumns: List[ColumnInputSpec] = mInputColumns.get

  //Create a HashSet of paged column names.
  private val mPagedColumnSet: Map[FijiColumnName, PagingSpec] =
      HashMap(
          inputColumns
              .filterNot { col: ColumnInputSpec => PagingSpec.Off.equals(col.pagingSpec)}
              .map { col: ColumnInputSpec => col.columnName -> col.pagingSpec}:_*)

  /**
   * Sets any configuration options that are required for running a MapReduce job that reads from a
   * Fiji table. This method gets called on the client machine during job setup.
   *
   * @param flow The cascading flow being built.
   * @param tap  The tap that is being used with this scheme.
   * @param conf The Job configuration to which FijiDataRequest will be added.
   */
  override def sourceConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[FijiRowData]],
          OutputCollector[_, _]],
      conf: JobConf
  ): Unit = {
    // Build a data request.
    val request: FijiDataRequest = ResourceUtil.withFijiTable(uri, conf) { table =>
      BaseFijiScheme.buildRequest(table.getLayout, timeRange, inputColumns)
    }
    // Write all the required values to the job's configuration object.
    BaseFijiScheme.configureFijiRowScan(uri, conf, rowRangeSpec, rowFilterSpec)
    // Set data request.
    conf.set(
        FijiConfKeys.FIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }

  /**
   * Reads and converts a row from a Fiji table to a Cascading Tuple. This method
   * is called once for each row on the cluster.
   *
   * @param flow The current Cascading flow being run.
   * @param sourceCall The source call for the flow that contains the context for this source.
   * @return `true` if another row was read and it was converted to a tuple,
   *     `false` if there were no more rows to read.
   */
  override def source(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[
          FijiSourceContext,
          RecordReader[Container[JEntityId], Container[FijiRowData]]]
  ): Boolean = {

    // Get the current key/value pair.
    val rowContainer = sourceCall.getContext.rowContainer
    // Get the next row.
    if (sourceCall.getInput.next(null, rowContainer)) {
      val row: FijiRowData = rowContainer.getContents
      // Build a tuple from this row.
      val result: Tuple = rowToTuple(row, mPagedColumnSet)

      sourceCall.getIncomingEntry.setTuple(result)
      flow.increment(BaseFijiScheme.CounterGroupName, BaseFijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the RecordReader.
    }
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow The current Cascading flow being run.
   * @param sinkCall The sink call for the flow that contains the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputCollector[_, _]]
  ): Unit = {
    ResourceUtil.withFijiTable(uri, flow.getConfigCopy) { table =>
      // Set the sink context to an opened FijiTableWriter.
      sinkCall.setContext(
          DirectFijiSinkContext(
              EntityIdFactory.getFactory(table.getLayout),
              table.getWriterFactory.openBufferedWriter()))
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Fiji table.
   *
   * @param flow The current Cascading flow being run.
   * @param sinkCall The sink call for the flow that contains the context for this source.
   */
  override def sink(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputCollector[_, _]]
  ): Unit = {
    val DirectFijiSinkContext(eidFactory, writer) = sinkCall.getContext

    def writeSingleValue(singleVal:ExpressColumnOutput[_]): Unit = {
      singleVal.version match {
        case Some(timestamp) =>
          writer.put(
              singleVal.entityId.toJavaEntityId(eidFactory),
              singleVal.family,
              singleVal.qualifier,
              timestamp,
              singleVal.encode (singleVal.datum))
        case None =>
          writer.put(
              singleVal.entityId.toJavaEntityId(eidFactory),
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
          writeSingleValue(anyVal)
        }
      case _ => throw new RuntimeException("Incorrect type. " +
          "The typed sink expects a type Iterable[ExpressColumnOutput[_]]")
    }
  }
}

/**
 * Companion object for TypedFijiScheme containing utility methods.
 */
object TypedFijiScheme {
  /**
   * Converts a row of requested data from FijiTable to a cascading Tuple.
   *
   * The row of results is wrapped in a [[com.moz.fiji.express.flow.ExpressResult]] object before adding
   * it to the cascading tuple. ExpressResult contains methods to access and retrieve the row data.
   * The wrapping of row data is done to enforce a [[ExpressResult]] type for all tuples being read
   * from [[com.moz.fiji.express.flow.TypedFijiSource]] for the type safe api.
   *
   * @param row The row data to be converted to a tuple.
   * @param pagedColumnMap A Map containing the PagingSpecs of the columns that are requested paged.
   * @return A tuple containing the requested values from row.
   */
  private[express] def rowToTuple(
      row: FijiRowData,
      pagedColumnMap: Map[FijiColumnName, PagingSpec]
  ): Tuple = {
    val result: Tuple = new Tuple()
    result.add(ExpressResult(row, pagedColumnMap))
    result
  }
}
