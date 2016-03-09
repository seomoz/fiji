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

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.ColumnFamilyOutputSpec
import com.moz.fiji.express.flow.ColumnOutputSpec
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.QualifiedColumnOutputSpec
import com.moz.fiji.express.flow.framework.FijiScheme
import com.moz.fiji.express.flow.framework.serialization.FijiKryoExternalizer
import com.moz.fiji.express.flow.util.ResourceUtil._
import com.moz.fiji.mapreduce.framework.HFileKeyValue
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.impl.DefaultFijiCellEncoderFactory
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator
import com.moz.fiji.schema.layout.impl.CellEncoderProvider
import com.moz.fiji.schema.{EntityId => JEntityId}

/**
 * A Fiji-specific implementation of a Cascading `Scheme` which defines how to write data
 * to HFiles.
 *
 * HFileFijiScheme is responsible for converting rows from a Fiji table that are input to a
 * Cascading flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to an HFile capable of being bulk loaded into HBase
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * HFileFijiScheme must be used with [[com.moz.fiji.express.flow.framework.hfile.HFileFijiTap]],
 * since it expects the Tap to have access to a Fiji table.
 * [[com.moz.fiji.express.flow.framework.hfile.HFileFijiSource]] handles the creation of both
 * HFileFijiScheme and FijiTap in FijiExpress.
 *
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param ocolumns mapping tuple field names to requests for Fiji columns.
 */
@ApiAudience.Framework
@ApiStability.Stable
final private[express] class HFileFijiScheme(
  private[express] val timestampField: Option[Symbol],
  ocolumns: Map[String, ColumnOutputSpec])
    extends Scheme[JobConf, RecordReader[_, _],
        OutputCollector[HFileKeyValue, NullWritable], Nothing, HFileFijiSinkContext] {

  import FijiScheme._

  /** Serialization workaround.  Do not access directly. */
  private[this] val _outputColumns = FijiKryoExternalizer(ocolumns)

  def outputColumns: Map[String, ColumnOutputSpec] = _outputColumns.get

  setSinkFields(buildSinkFields(outputColumns, timestampField))

  override def sourceConfInit(
      flowProcess: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[_, _], OutputCollector[HFileKeyValue, NullWritable]],
      conf: JobConf): Unit =
    throw new UnsupportedOperationException("Cannot read from HFiles")

  override def source(
      flowProcess: FlowProcess[JobConf],
      sourceCall: SourceCall[Nothing, RecordReader[_, _]]): Boolean =
    throw new UnsupportedOperationException("Cannot read from HFiles")

  /**
   * Sets any configuration options that are required for running a MapReduce job that writes to a
   * Fiji table. This method gets called on the client machine during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our FijiDataRequest.
   */
  override def sinkConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[_, _], OutputCollector[HFileKeyValue, NullWritable]],
      conf: JobConf) {
    // No-op since no configuration parameters need to be set to encode data for Fiji.
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[HFileFijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val conf = flow.getConfigCopy
    val uri: FijiURI = FijiURI.newBuilder(conf.get(FijiConfKeys.FIJI_OUTPUT_TABLE_URI)).build()

    val fiji: Fiji = Fiji.Factory.open(uri, conf)

    withFijiTable(fiji, uri.getTable) { table =>
      val layout = table.getLayout
      sinkCall.setContext(
        HFileFijiSinkContext(
          EntityIdFactory.getFactory(layout),
          HBaseColumnNameTranslator.from(layout),
          new CellEncoderProvider(uri, layout, fiji.getSchemaTable,
            DefaultFijiCellEncoderFactory.get()),
          fiji))
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Fiji table. This method is called once
   * for each row on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[HFileFijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val HFileFijiSinkContext(eidFactory, columnTranslator, encoderProvider, _) = sinkCall.getContext
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

      val fijiColumn = new FijiColumnName(column.columnName.getFamily, qualifier)
      val hbaseColumn = columnTranslator.toHBaseColumnName(fijiColumn)
      val encoder = encoderProvider.getEncoder(fijiColumn.getFamily, fijiColumn.getQualifier)

      val hfileKV = new HFileKeyValue(
        eid.getHBaseRowKey,
        hbaseColumn.getFamily,
        hbaseColumn.getQualifier,
        version,
        encoder.encode(column.encode(value)))

      sinkCall.getOutput.collect(hfileKV, NullWritable.get)
    }
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkCleanup(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[HFileFijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {
    sinkCall.getContext.fiji.release()
    sinkCall.setContext(null)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: FijiScheme => (
        outputColumns == other.outputColumns
        && timestampField == other.timestampField)
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(outputColumns, timestampField)

  override def toString: String =
    Objects
      .toStringHelper(this)
      .add("outputColumns", outputColumns)
      .add("timestampField", timestampField)
      .toString
}

/**
 * Context housing information necessary for the scheme to interact
 * with the Fiji table.
 */
@ApiAudience.Framework
@ApiStability.Stable
private[express] final case class HFileFijiSinkContext (
    eidFactory: EntityIdFactory,
    columnTranslator: HBaseColumnNameTranslator,
    encoderProvider: CellEncoderProvider,
    fiji: Fiji)
