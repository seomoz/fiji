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

package com.moz.fiji.express.flow.framework

import java.io.InputStream
import java.io.OutputStream
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.express.flow.ColumnFamilyOutputSpec
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.ColumnOutputSpec
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.QualifiedColumnOutputSpec
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.TimeRangeSpec
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.{EntityId => JEntityId}
import com.moz.fiji.express.flow.util.ResourceUtil.withFijiTable

/**
 * A local version of [[com.moz.fiji.express.flow.framework.FijiScheme]] that is meant to be used with
 * Cascading's local job runner. [[com.moz.fiji.express.flow.framework.FijiScheme]] and
 * LocalFijiScheme both define how to read and write the data stored in a Fiji table.
 *
 * [[LocalFijiScheme]] extends trait [[com.moz.fiji.express.flow.framework.BaseLocalFijiScheme]],
 * which holds method implementations of [[cascading.scheme.Scheme]] that are common to both
 * [[LocalFijiScheme]] and [[TypedLocalFijiScheme]] for running jobs locally.
 *
 * This scheme is meant to be used with [[com.moz.fiji.express.flow.framework.LocalFijiTap]] and
 * Cascading's local job runner. Jobs run with Cascading's local job runner execute on
 * your local machine instead of a cluster. This can be helpful for testing or quick jobs.
 *
 * In FijiExpress, LocalFijiScheme is used in tests.  See [[com.moz.fiji.express.flow.FijiSource]]'s
 * `TestLocalFijiScheme` class.
 *
 * This scheme is responsible for converting rows from a Fiji table that are input to a
 * Cascading flow into Cascading tuples (see
 * `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Fiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * @see [[com.moz.fiji.express.flow.framework.FijiScheme]]
 * @see [[com.moz.fiji.express.flow.framework.BaseLocalFijiScheme]]
 *
 * @param uri of table to be written to.
 * @param timeRange to include from the Fiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param inputColumns is a one-to-one mapping from field names to Fiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 * @param outputColumns is a one-to-one mapping from field names to Fiji columns. Values from the
 *     tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
private[express] case class LocalFijiScheme(
    private[express] val uri: FijiURI,
    private[express] val timeRange: TimeRangeSpec,
    private[express] val timestampField: Option[Symbol],
    private[express] val inputColumns: Map[String, ColumnInputSpec] = Map(),
    private[express] val outputColumns: Map[String, ColumnOutputSpec] = Map(),
    private[express] val rowRangeSpec: RowRangeSpec,
    private[express] val rowFilterSpec: RowFilterSpec)
 extends BaseLocalFijiScheme {

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(FijiScheme.buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(FijiScheme.buildSinkFields(outputColumns, timestampField))

  /**
   * Sets up any resources required to read from a Fiji table.
   *
   * @param process currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {

    val jobConfiguration: JobConf =
        HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))
    withFijiTable(uri, jobConfiguration) { table: FijiTable =>
      // Build the input context.
      val request = BaseFijiScheme.buildRequest(table.getLayout, timeRange, inputColumns.values)
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
   * Reads and converts a row from a Fiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *     <code>false</code> if there were no more rows to read.
   */
  override def source(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]): Boolean = {
    val context: InputContext = sourceCall.getContext
    if (context.iterator.hasNext) {
      // Get the current row.
      val row: FijiRowData = context.iterator.next()
      val result: Tuple = FijiScheme.rowToTuple(
          inputColumns,
          getSourceFields,
          timestampField,
          row)

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      process.increment(BaseFijiScheme.CounterGroupName, BaseFijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
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
   * Converts and writes a Cascading Tuple to a Fiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sink(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputStream]) {
    val DirectFijiSinkContext(eidFactory, writer) = sinkCall.getContext
    val tuple: TupleEntry = sinkCall.getOutgoingEntry

    // Get the entityId.
    val eid: JEntityId = tuple
        .getObject(FijiScheme.EntityIdField)
        .asInstanceOf[EntityId]
        .toJavaEntityId(eidFactory)

    // Get a timestamp to write the values to, if it was specified by the user.
    val version: Long = timestampField
        .map(field => tuple.getLong(field.name))
        .getOrElse(HConstants.LATEST_TIMESTAMP)

    outputColumns.foreach { case (field, column) =>
      val value = tuple.getObject(field)

      val qualifier: String = column match {
        case qc: QualifiedColumnOutputSpec => qc.qualifier
        case cf: ColumnFamilyOutputSpec => tuple.getString(cf.qualifierSelector.name)
      }
      writer.put(eid, column.columnName.getFamily, qualifier, version, column.encode(value))
    }
  }
}
