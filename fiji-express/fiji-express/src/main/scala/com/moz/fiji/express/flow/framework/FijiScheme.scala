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

import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.flow.FlowProcess
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.express.flow.TimeRangeSpec
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.ColumnOutputSpec
import com.moz.fiji.express.flow.ColumnFamilyOutputSpec
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.FlowCell
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.ColumnFamilyInputSpec
import com.moz.fiji.express.flow.QualifiedColumnInputSpec
import com.moz.fiji.express.flow.QualifiedColumnOutputSpec
import com.moz.fiji.express.flow.PagingSpec
import com.moz.fiji.express.flow.TransientStream
import com.moz.fiji.express.flow.framework.serialization.FijiKryoExternalizer
import com.moz.fiji.express.flow.util.AvroUtil
import com.moz.fiji.express.flow.util.ResourceUtil.withFijiTable
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.schema.ColumnVersionIterator
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.MapFamilyVersionIterator
import com.moz.fiji.schema.{EntityId => JEntityId}
import com.moz.fiji.schema.FijiURI

/**
 * A Fiji-specific implementation of a Cascading `Scheme` for the fields API, which defines how to
 * read and write the data stored in a Fiji table.
 *
 * [[FijiScheme]] extends trait [[com.moz.fiji.express.flow.framework.BaseFijiScheme]], which holds
 * method implementations of [[cascading.scheme.Scheme]] that are common to both [[FijiScheme]]
 * and [[TypedFijiScheme]] for running mapreduce jobs.
 *
 * FijiScheme is responsible for converting rows from a Fiji table that are input to a Cascading
 * flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Fiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * FijiScheme must be used with [[com.moz.fiji.express.flow.framework.FijiTap]],
 * since it expects the Tap to have access to a Fiji table. [[com.moz.fiji.express.flow.FijiSource]]
 * handles the creation of both FijiScheme and FijiTap in FijiExpress.
 *
 * @see[[com.moz.fiji.express.flow.framework.BaseFijiScheme]]
 *
 * @param tableAddress of the target Fiji table.
 * @param timeRange to include from the Fiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param icolumns mapping tuple field names to requests for Fiji columns.
 * @param ocolumns mapping tuple field names to specifications for Fiji columns to write out to.
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
class FijiScheme(
    private[express] val tableAddress: String,
    private[express] val timeRange: TimeRangeSpec,
    private[express] val timestampField: Option[Symbol],
    icolumns: Map[String, ColumnInputSpec] = Map(),
    ocolumns: Map[String, ColumnOutputSpec] = Map(),
    private[express] val rowRangeSpec: RowRangeSpec,
    private[express] val rowFilterSpec: RowFilterSpec
) extends BaseFijiScheme {
  import FijiScheme._

  private def uri: FijiURI = FijiURI.newBuilder(tableAddress).build()

  /** Serialization workaround.  Do not access directly. */
  private[this] val _inputColumns = FijiKryoExternalizer(icolumns)
  private[this] val _outputColumns = FijiKryoExternalizer(ocolumns)

  def inputColumns: Map[String, ColumnInputSpec] = _inputColumns.get
  def outputColumns: Map[String, ColumnOutputSpec] = _outputColumns.get

  // Including output column keys here because we might need to read back outputs during test
  // TODO (EXP-250): Ideally we should include outputColumns.keys here only during tests.
  setSourceFields(buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(buildSinkFields(outputColumns, timestampField))

  /**
   * Sets any configuration options that are required for running a MapReduce job that reads from a
   * Fiji table. This method gets called on the client machine during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our FijiDataRequest.
   */
  override def sourceConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[FijiRowData]],
          OutputCollector[_, _]],
      conf: JobConf
  ) {
    // Build a data request.
    val request: FijiDataRequest = withFijiTable(uri, conf) { table =>
      BaseFijiScheme.buildRequest(table.getLayout, timeRange, inputColumns.values)
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
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
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
      val result: Tuple = rowToTuple(inputColumns, getSourceFields, timestampField, row)

      // If no fields were missing, set the result tuple and return from this method.
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
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputCollector[_, _]]
  ): Unit =  {
    withFijiTable(uri, flow.getConfigCopy) { table =>
      // Set the sink context to an opened FijiTableWriter.
      sinkCall.setContext(
          DirectFijiSinkContext(
              EntityIdFactory.getFactory(table.getLayout),
              table.getWriterFactory.openBufferedWriter()))
      }
  }

  /**
   * Converts and writes a Cascading Tuple to a Fiji table. This method is called once
   * for each row on the cluster, so it should be kept as light as possible.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputCollector[_, _]]) {
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

  override def equals(obj: Any): Boolean = obj match {
    case other: FijiScheme => (
        inputColumns == other.inputColumns
        && outputColumns == other.outputColumns
        && timestampField == other.timestampField
        && timeRange == other.timeRange)
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(
      inputColumns,
      outputColumns,
      timeRange,
      timestampField)
}

/**
 * Companion object for FijiScheme.
 *
 * Contains constants and helper methods for converting between Fiji rows and Cascading tuples,
 * building Fiji data requests, and some utility methods for handling Cascading fields.
 */
@ApiAudience.Framework
@ApiStability.Stable
object FijiScheme {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FijiScheme])

  /** Field name containing a row's [[com.moz.fiji.schema.EntityId]]. */
  val EntityIdField: String = "entityId"

  /**
   * Converts a FijiRowData to a Cascading tuple.
   *
   * If there is no data in a column in this row, the value of that field in the tuple will be an
   * empty iterable of cells.
   *
   * This is used in the `source` method of FijiScheme, for reading rows from Fiji into
   * FijiExpress tuples.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of desired tuple elements.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @param row to convert to a tuple.
   * @return a tuple containing the values contained in the specified row.
   */
  private[express] def rowToTuple(
      columns: Map[String, ColumnInputSpec],
      fields: Fields,
      timestampField: Option[Symbol],
      row: FijiRowData
  ): Tuple = {
    val result: Tuple = new Tuple()

    // Add the row's EntityId to the tuple.
    val entityId: EntityId = EntityId.fromJavaEntityId(row.getEntityId)
    result.add(entityId)

    def rowToTupleColumnFamily(cf: ColumnFamilyInputSpec) {
      cf.pagingSpec match {
        case PagingSpec.Off => {
          val stream: Seq[FlowCell[_]] = row
              .iterator(cf.family)
              .asScala
              .toList
              .map { fijiCell: FijiCell[_] => FlowCell(fijiCell) }
          result.add(stream)
        }
        case PagingSpec.Cells(pageSize) => {
          def genItr(): Iterator[FlowCell[_]] = {
            new MapFamilyVersionIterator(row, cf.family, BaseFijiScheme.qualifierPageSize, pageSize)
                .asScala
                .map { entry: MapFamilyVersionIterator.Entry[_] =>
                  FlowCell(
                      cf.family,
                      entry.getQualifier,
                      entry.getTimestamp,
                      AvroUtil.avroToScala(entry.getValue))
            }
          }
          result.add(new TransientStream(genItr))
        }
      }
    }

    def rowToTupleQualifiedColumn(qc: QualifiedColumnInputSpec) {
      qc.pagingSpec match {
        case PagingSpec.Off => {
          val stream: Seq[FlowCell[_]] = row
              .iterator(qc.family, qc.qualifier)
              .asScala
              .toList
              .map { fijiCell: FijiCell[_] => FlowCell(fijiCell) }
          result.add(stream)
        }
        case PagingSpec.Cells(pageSize) => {
          def genItr(): Iterator[FlowCell[_]] = {
            new ColumnVersionIterator(row, qc.family, qc.qualifier, pageSize)
                .asScala
                .map { entry: java.util.Map.Entry[java.lang.Long,_] =>
                  FlowCell(
                    qc.family,
                    qc.qualifier,
                    entry.getKey,
                    AvroUtil.avroToScala(entry.getValue))
                }
          }
          result.add(new TransientStream(genItr))
        }
      }
    }

    // Get rid of the entity id and timestamp fields, then map over each field to add a column
    // to the tuple.
    fields
        .iterator()
        .asScala
        .filter { field => field.toString != EntityIdField }
        .filter { field => field.toString != timestampField.getOrElse("") }
        .map { field => columns(field.toString) }
        // Build the tuple, by adding each requested value into result.
        .foreach {
          case cf: ColumnFamilyInputSpec => rowToTupleColumnFamily(cf)
          case qc: QualifiedColumnInputSpec => rowToTupleQualifiedColumn(qc)
        }
    result
  }

  /**
   * Transforms a list of field names into a Cascading [[cascading.tuple.Fields]] object.
   *
   * @param fieldNames is a list of field names.
   * @return a Fields object containing the names.
   */
  private def toField(fieldNames: Iterable[Comparable[_]]): Fields = {
    new Fields(fieldNames.toArray:_*)
  }

  /**
   * Builds the list of tuple fields being read by a scheme. The special field name
   * "entityId" will be included to hold entity ids from the rows of Fiji tables.
   *
   * @param fieldNames is a list of field names that a scheme should read.
   * @return is a collection of fields created from the names.
   */
  private[express] def buildSourceFields(fieldNames: Iterable[String]): Fields = {
    toField(Set(EntityIdField) ++ fieldNames)
  }

  /**
   * Builds the list of tuple fields being written by a scheme. The special field name "entityId"
   * will be included to hold entity ids that values should be written to. Any fields that are
   * specified as qualifiers for a map-type column family will also be included. A timestamp field
   * can also be included, identifying a timestamp that all values will be written to.
   *
   * @param columns is the column requests for this Scheme, with the names of each of the
   *     fields that contain data to write to Fiji.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @return a collection of fields created from the parameters.
   */
  private[express] def buildSinkFields(
      columns: Map[String, ColumnOutputSpec],
      timestampField: Option[Symbol]
  ): Fields = {
    toField(Set(EntityIdField)
        ++ columns.keys
        ++ extractQualifierSelectors(columns)
        ++ timestampField.map { _.name } )
  }

  /**
   * Extracts the names of qualifier selectors from the column requests for a Scheme.
   *
   * @param columns is the column requests for a Scheme.
   * @return the names of fields that are qualifier selectors.
   */
  private[express] def extractQualifierSelectors(
      columns: Map[String, ColumnOutputSpec]
  ): Iterator[String] =
    columns.valuesIterator.collect {
      case x: ColumnFamilyOutputSpec => x.qualifierSelector.name
    }
}

