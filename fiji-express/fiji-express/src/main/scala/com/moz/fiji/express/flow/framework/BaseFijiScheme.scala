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

import scala.Some
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.JobConf
import org.apache.commons.codec.binary.Base64

import cascading.flow.FlowProcess
import cascading.tap.Tap
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.scheme.Scheme

import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.express.flow.ColumnFamilyInputSpec
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.QualifiedColumnInputSpec
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.express.flow.SchemaSpec
import com.moz.fiji.express.flow.TimeRangeSpec
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.schema.avro.SchemaType
import com.moz.fiji.schema.{EntityId => JEntityId}
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.filter.FijiColumnFilter
import com.moz.fiji.schema.layout.ColumnReaderSpec
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.FijiBufferedWriter
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiDataRequestBuilder
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiURI


/**
 * A Base trait containing Fiji-specific implementation of a Cascading `Scheme` that is common for
 * both the Fields API, and the Type-safe API. Scheme's [[FijiScheme]] and [[TypedFijiScheme]]
 * extend this trait and share the implemented methods.
 */
private[express] trait BaseFijiScheme
extends Scheme[
    JobConf,
    RecordReader[Container[JEntityId], Container[FijiRowData]],
    OutputCollector[_, _],
    FijiSourceContext,
    DirectFijiSinkContext] {

  /**
   * Sets up any resources required for the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[
          FijiSourceContext,
          RecordReader[Container[JEntityId], Container[FijiRowData]]]) {
    // Set the context used when reading data from the source.
    sourceCall.setContext(FijiSourceContext(sourceCall.getInput.createValue()))
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourceCleanup(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[
          FijiSourceContext,
          RecordReader[Container[JEntityId], Container[FijiRowData]]]) {
    sourceCall.setContext(null)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Fiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our FijiDataRequest.
   */
  override def sinkConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[FijiRowData]],
          OutputCollector[_, _]],
      conf: JobConf) {
    // No-op since no configuration parameters need to be set to encode data for Fiji.
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkCleanup(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputCollector[_, _]]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }

}

/**
 * Companion object for the [[BaseFijiScheme]].
 */
object BaseFijiScheme {

  /** Default number of qualifiers to retrieve when paging in a map type family.*/
  private[express] val qualifierPageSize: Int = 1000
  /** Hadoop mapred counter group for FijiExpress. */
  private[express] val CounterGroupName = "fiji-express"
  /** Counter name for the number of rows successfully read. */
  private[express] val CounterSuccess = "ROWS_SUCCESSFULLY_READ"

  /**
   * Builds a data request out of the timerange and list of column requests.
   *
   * @param timeRange of cells to retrieve.
   * @param columns to retrieve.
   * @return data request configured with timeRange and columns.
   */
  private[express] def buildRequest(
      layout: FijiTableLayout,
      timeRange: TimeRangeSpec,
      columns: Iterable[ColumnInputSpec]
  ): FijiDataRequest = {

    def addColumn(
        builder: FijiDataRequestBuilder,
        column: ColumnInputSpec
        ): FijiDataRequestBuilder.ColumnsDef = {
      val fijiFilter: FijiColumnFilter = column
          .filterSpec
          .toFijiColumnFilter
          .getOrElse(null)
      val columnReaderSpec: ColumnReaderSpec = {
        // Check and ensure that this column isn't a counter, protobuf, or raw bytes encoded column.
        // If it is, ignore the provided schema spec.
        val schemaType = column match {
          case QualifiedColumnInputSpec(family, qualifier, _, _, _, _) => {
            // If this fully qualified column is actually part of a map-type column family,
            // then get the schema type from the map-type column family instead. Otherwise get it
            // from the qualified column as usual.
            val columnFamily = layout
                .getFamilyMap
                .get(column.columnName.getFamily)
            if (columnFamily.isMapType) {
              columnFamily
                  .getDesc
                  .getMapSchema
                  .getType
            } else {
              columnFamily
                  .getColumnMap
                  .get(column.columnName.getQualifier)
                  .getDesc
                  .getColumnSchema
                  .getType
            }
          }
          case ColumnFamilyInputSpec(family, _, _, _, _) => {
            layout
                .getFamilyMap
                .get(column.columnName.getFamily)
                .getDesc
                .getMapSchema
                .getType
          }
        }
        schemaType match {
          case SchemaType.COUNTER => ColumnReaderSpec.counter()
          case SchemaType.PROTOBUF => ColumnReaderSpec.protobuf()
          case SchemaType.RAW_BYTES => ColumnReaderSpec.bytes()
          case _ => column.schemaSpec match {
            case SchemaSpec.DefaultReader => ColumnReaderSpec.avroDefaultReaderSchemaGeneric()
            case SchemaSpec.Writer => ColumnReaderSpec.avroWriterSchemaGeneric()
            case SchemaSpec.Generic(schema) => ColumnReaderSpec.avroReaderSchemaGeneric(schema)
            case SchemaSpec.Specific(record) => ColumnReaderSpec.avroReaderSchemaSpecific(record)
          }
        }
      }
      builder.newColumnsDef()
          .withMaxVersions(column.maxVersions)
          .withFilter(fijiFilter)
          .withPageSize(column.pagingSpec.cellsPerPage.getOrElse(0))
          .add(column.columnName, columnReaderSpec)
    }

    val requestBuilder: FijiDataRequestBuilder = FijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columns.foreach(column => addColumn(requestBuilder, column))
    requestBuilder.build()
  }


  /**
   * Sets configuration parameters for a purposing a row scan to read data from the Fiji
   * table.
   *
   * @param uri The uri for the target table.
   * @param conf The configuration to which the datarequest is added.
   * @param rowRangeSpec The specification of the row range for the request.
   * @param rowFilterSpec The specification of the filters for the request.
   */
  def configureFijiRowScan(
    uri: FijiURI,
    conf: JobConf,
    rowRangeSpec: RowRangeSpec,
    rowFilterSpec: RowFilterSpec
  ): Unit = {
    val eidFactory = ResourceUtil.withFijiTable(uri, conf) { table =>
      EntityIdFactory.getFactory(table.getLayout)
    }
    // Set start entity id.
    rowRangeSpec.startEntityId match {
      case Some(entityId) =>
        conf.set(
          FijiConfKeys.FIJI_START_ROW_KEY,
          Base64.encodeBase64String(
            entityId.toJavaEntityId(eidFactory).getHBaseRowKey))
      case None => //Do Nothing
    }
    // Set limit entity id.
    rowRangeSpec.limitEntityId match {
      case Some(entityId) =>
        conf.set(
          FijiConfKeys.FIJI_LIMIT_ROW_KEY,
          Base64.encodeBase64String(
            entityId.toJavaEntityId(eidFactory).getHBaseRowKey))
      case None => //Do Nothing
    }
    // Set row filter.
    rowFilterSpec.toFijiRowFilter match {
      case Some(fijiRowFilter) =>
        conf.set(FijiConfKeys.FIJI_ROW_FILTER, fijiRowFilter.toJson.toString)
      case None => //Do Nothing
    }
  }
}

/**
 * Container for a Fiji row data and Fiji table layout object that is required by a map reduce
 * task while reading from a Fiji table.
 *
 * @param rowContainer is the representation of a Fiji row.
 */
@ApiAudience.Private
@ApiStability.Stable
private[express] final case class FijiSourceContext(rowContainer: Container[FijiRowData])

/**
 * Container for the table writer and Fiji table layout required during a sink
 * operation to write the output of a map reduce task back to a Fiji table.
 * This is configured during the sink prepare operation.
 */
@ApiAudience.Private
@ApiStability.Stable
private[express] final case class DirectFijiSinkContext(
    eidFactory: EntityIdFactory,
    writer: FijiBufferedWriter)
