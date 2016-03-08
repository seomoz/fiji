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

package com.moz.fiji.express.flow


import java.io.OutputStream
import java.util.Properties

import scala.Some
import scala.collection.mutable.Buffer
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import cascading.flow.FlowProcess
import cascading.scheme.SinkCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Write
import com.twitter.scalding.Test
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.google.common.base.Objects
import cascading.flow.hadoop.util.HadoopUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability

import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTableReader
import com.moz.fiji.schema.FijiTableWriter
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiRowScanner
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.layout.ColumnReaderSpec
import com.moz.fiji.schema.FijiDataRequest.Column
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions
import com.moz.fiji.express.flow.framework.BaseFijiScheme
import com.moz.fiji.express.flow.framework.DirectFijiSinkContext
import com.moz.fiji.express.flow.framework.FijiTap
import com.moz.fiji.express.flow.framework.FijiScheme
import com.moz.fiji.express.flow.framework.LocalFijiTap
import com.moz.fiji.express.flow.framework.LocalFijiScheme
import com.moz.fiji.express.flow.util.ResourceUtil


/**
 * A read or write view of a Fiji table.
 *
 * A Scalding `Source` provides a view of a data source that can be read as Scalding tuples. It
 * is comprised of a Cascading tap [[cascading.tap.Tap]], which describes where the data is and how
 * to access it, and a Cascading Scheme [[cascading.scheme.Scheme]], which describes how to read
 * and interpret the data.
 *
 * When reading from a Fiji table, a `FijiSource` will provide a view of a Fiji table as a
 * collection of tuples that correspond to rows from the Fiji table. Which columns will be read
 * and how they are associated with tuple fields can be configured,
 * as well as the time span that cells retrieved must belong to.
 *
 * When writing to a Fiji table, a `FijiSource` views a Fiji table as a collection of tuples that
 * correspond to cells from the Fiji table. Each tuple to be written must provide a cell address
 * by specifying a Fiji `EntityID` in the tuple field `entityId`, a value to be written in a
 * configurable field, and (optionally) a timestamp in a configurable field.
 *
 * End-users cannot directly obtain instances of `FijiSource`. Instead,
 * they should use the factory methods provided as part of the [[com.moz.fiji.express.flow]] module.
 *
 * @param tableAddress is a Fiji URI addressing the Fiji table to read or write to.
 * @param timeRange that cells read must belong to. Ignored when the source is used to write.
 * @param timestampField is the name of a tuple field that will contain cell timestamp when the
 *     source is used for writing. Specify `None` to write all cells at the current time.
 * @param inputColumns is a one-to-one mapping from field names to Fiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 * @param outputColumns is a one-to-one mapping from field names to Fiji columns. Values from the
 *     tuple fields will be written to their associated column.
 * @param rowRangeSpec is the specification for which interval of rows to scan.
 * @param rowFilterSpec is the specification for which row filter to apply.
 */
@ApiAudience.Framework
@ApiStability.Stable
final class FijiSource private[express] (
    val tableAddress: String,
    val timeRange: TimeRangeSpec,
    val timestampField: Option[Symbol],
    val inputColumns: Map[Symbol, ColumnInputSpec] = Map(),
    val outputColumns: Map[Symbol, ColumnOutputSpec] = Map(),
    val rowRangeSpec: RowRangeSpec = RowRangeSpec.All,
    val rowFilterSpec: RowFilterSpec = RowFilterSpec.NoFilter
) extends Source {
  import FijiSource._

  /** The URI of the target Fiji table. */
  private val uri: FijiURI = FijiURI.newBuilder(tableAddress).build()

  /** A Fiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  val fijiScheme: FijiScheme =
      new FijiScheme(
          tableAddress,
          timeRange,
          timestampField,
          convertKeysToStrings(inputColumns),
          convertKeysToStrings(outputColumns),
          rowRangeSpec,
          rowFilterSpec)

  /** A Fiji scheme intended to be used with Scalding/Cascading's local mode. */
  val localFijiScheme: LocalFijiScheme =
      new LocalFijiScheme(
          uri,
          timeRange,
          timestampField,
          convertKeysToStrings(inputColumns),
          convertKeysToStrings(outputColumns),
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
    /** Combination of normal input columns and input versions of the output columns (the latter are
     * needed for reading back written results) */
    def getInputColumnsForTesting: Map[String, ColumnInputSpec] = {
      val columnsFromReads = inputColumnSpecifyAllData(convertKeysToStrings(inputColumns))
      val columnsFromWrites = inputColumnSpecifyAllData(
        convertKeysToStrings(outputColumns)
          .mapValues { colSpec: ColumnOutputSpec =>
            ColumnInputSpec(colSpec.columnName.toString, schemaSpec = colSpec.schemaSpec)
          })
      columnsFromReads ++ columnsFromWrites
    }

    mode match {
      // Production taps.
      case Hdfs(_,_) => new FijiTap(uri, fijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalFijiTap(uri, localFijiScheme).asInstanceOf[Tap[_, _, _]]

      // Test taps.
      case HadoopTest(conf, buffers) => readOrWrite match {
        case Read => {
          val scheme = fijiScheme
          populateTestTable(uri, buffers(this), scheme.getSourceFields, conf)

          new FijiTap(uri, scheme).asInstanceOf[Tap[_, _, _]]
        }
        case Write => {
          val scheme = new TestFijiScheme(
              uri,
              timestampField,
              getInputColumnsForTesting,
              convertKeysToStrings(outputColumns),
              rowRangeSpec,
              rowFilterSpec)

          new FijiTap(uri, scheme).asInstanceOf[Tap[_, _, _]]
        }
      }
      case Test(buffers) => readOrWrite match {
        // Use Fiji's local tap and scheme when reading.
        case Read => {
          val scheme = localFijiScheme
          populateTestTable(
              uri,
              buffers(this),
              scheme.getSourceFields,
              HBaseConfiguration.create())

          new LocalFijiTap(uri, scheme).asInstanceOf[Tap[_, _, _]]
        }

        // After performing a write, use TestLocalFijiScheme to populate the output buffer.
        case Write => {
          val scheme = new TestLocalFijiScheme(
              buffers(this),
              uri,
              timeRange,
              timestampField,
              getInputColumnsForTesting,
              convertKeysToStrings(outputColumns),
              rowRangeSpec,
              rowFilterSpec)

          new LocalFijiTap(uri, scheme).asInstanceOf[Tap[_, _, _]]
        }
      }

      case _ => throw new RuntimeException("Trying to create invalid tap")
    }
  }

 override def toString: String =
   Objects
       .toStringHelper(this)
       .add("tableAddress", tableAddress)
       .add("timeRangeSpec", timeRange)
       .add("timestampField", timestampField)
       .add("inputColumns", inputColumns)
       .add("outputColumns", outputColumns)
       .add("rowRangeSpec", rowRangeSpec)
       .add("rowFilterSpec", rowFilterSpec)
       .toString

  override def equals(obj: Any): Boolean = obj match {
    case other: FijiSource => (
          tableAddress == other.tableAddress
          && inputColumns == other.inputColumns
          && outputColumns == other.outputColumns
          && timestampField == other.timestampField
          && timeRange == other.timeRange
          && rowRangeSpec == other.rowRangeSpec
          && rowFilterSpec == other.rowFilterSpec)
    case _ => false
  }

  override def hashCode(): Int =
      Objects.hashCode(
          tableAddress,
          inputColumns,
          outputColumns,
          timestampField,
          timeRange,
          rowRangeSpec,
          rowFilterSpec)
}

/**
 * Contains a private, inner class used by [[com.moz.fiji.express.flow.FijiSource]] when working with
 * tests.
 */
@ApiAudience.Framework
@ApiStability.Stable
private[express] object FijiSource {

  /**
   * Construct a FijiSource and create a connection to the physical data source
   * (also known as a Tap in Cascading) which, in this case, is a [[com.moz.fiji.schema.FijiTable]].
   * This method is meant to be used by fiji-express-cascading's Java TapBuilder.
   * Scala users ought to construct and create their taps via the provided class methods.
   *
   * @param tableAddress is a Fiji URI addressing the Fiji table to read or write to.
   * @param timeRange that cells read must belong to. Ignored when the source is used to write.
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *     source is used for writing. Specify `None` to write all cells at the current time.
   * @param inputColumns is a one-to-one mapping from field names to Fiji columns.
   *     The columns in the map will be read into their associated tuple fields.
   * @param outputColumns is a one-to-one mapping from field names to Fiji columns. Values from the
   *     tuple fields will be written to their associated column.
   * @return A tap to use for this data source.
   */
  private[express] def makeTap(
      tableAddress: String,
      timeRange: TimeRangeSpec,
      timestampField: String,
      inputColumns: java.util.Map[String, ColumnInputSpec],
      outputColumns: java.util.Map[String, ColumnOutputSpec]
  ): Tap[_, _, _] = {
    val fijiSource = new FijiSource(
        tableAddress,
        timeRange,
        Option(Symbol(timestampField)),
        inputColumns.asScala.toMap
            .map{ case (symbolName, column) => (Symbol(symbolName), column) },
        outputColumns.asScala.toMap
            .map{ case (symbolName, column) => (Symbol(symbolName), column) }
    )
    new FijiTap(fijiSource.uri, fijiSource.fijiScheme)
  }

  /**
   * Convert scala columns definition into its corresponding java variety.
   *
   * @param columnMap Mapping from field name to Fiji column name.
   * @return Java map from field name to column definition.
   */
  private[express] def convertKeysToStrings[T <: Any](columnMap: Map[Symbol, T]): Map[String, T] =
    columnMap.map { case (symbol, column) => (symbol.name, column) }

  // Test specific code below here.
  /**
   * Takes a buffer containing rows and writes them to the table at the specified uri.
   *
   * @param tableUri of the table to populate.
   * @param rows Tuples to write to populate the table with.
   * @param fields Field names for elements in the tuple.
   * @param configuration defining the cluster to use.
   */
  private def populateTestTable(
      tableUri: FijiURI,
      rows: Option[Buffer[Tuple]],
      fields: Fields,
      configuration: Configuration) {
    ResourceUtil.doAndRelease(Fiji.Factory.open(tableUri)) { fiji: Fiji =>
      // Layout to get the default reader schemas from.
      val layout = ResourceUtil.withFijiTable(tableUri, configuration) { table: FijiTable =>
        table.getLayout
      }

      val eidFactory = EntityIdFactory.getFactory(layout)

      // Write the desired rows to the table.
      ResourceUtil.withFijiTableWriter(tableUri, configuration) { writer: FijiTableWriter =>
        rows.toSeq.flatten.foreach { row: Tuple =>
          val tupleEntry = new TupleEntry(fields, row)
          val iterator = fields.iterator()

          // Get the entity id field.
          val entityIdField = iterator.next().toString
          val entityId = tupleEntry
            .getObject(entityIdField)
            .asInstanceOf[EntityId]

          // Iterate through fields in the tuple, adding each one.
          while (iterator.hasNext) {
            val field = iterator.next().toString

            // Get the timeline to be written.
            val cells: Seq[FlowCell[Any]] = tupleEntry
                .getObject(field)
                .asInstanceOf[Seq[FlowCell[Any]]]

            // Write the timeline to the table.
            cells.foreach { cell: FlowCell[Any] =>
              writer.put(
                  entityId.toJavaEntityId(eidFactory),
                  cell.family,
                  cell.qualifier,
                  cell.version,
                  cell.datum
              )
            }
          }
        }
      }
    }
  }

  private[express] def newGetAllData(col: ColumnInputSpec): ColumnInputSpec = {
    ColumnInputSpec(
        col.columnName.toString,
        Integer.MAX_VALUE,
        col.filterSpec,
        col.pagingSpec,
        col.schemaSpec)
  }

  /**
   * Returns a map from field name to column input spec where the column input spec has been
   * configured as an output column.
   *
   * This is used in tests, when we use FijiScheme to read tuples from a Fiji table, and we want
   * to read all data in all of the columns, so the test can inspect all data in the table.
   *
   * @param columns to transform.
   * @return transformed map where the column input specs are configured for output.
   */
  private def inputColumnSpecifyAllData(
      columns: Map[String, ColumnInputSpec]): Map[String, ColumnInputSpec] = {
    columns.mapValues(newGetAllData)
        // Need this to make the Map serializable (issue with mapValues)
        .map(identity)
  }

  /**
   * A LocalFijiScheme that loads rows in a table into the provided buffer. This class
   * should only be used during tests.
   *
   * @param buffer to fill with post-job table rows for tests.
   * @param timeRange of timestamps to read from each column.
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *     source is used for writing. Specify `None` to write all cells at the current time.
   * @param inputColumns is a map of Scalding field name to ColumnInputSpec.
   * @param outputColumns is a map of ColumnOutputSpec to Scalding field name.
   * @param rowRangeSpec is the specification for which interval of rows to scan.
   * @param rowFilterSpec is the specification for which row filter to apply.
   */
  private class TestLocalFijiScheme(
      val buffer: Option[Buffer[Tuple]],
      uri: FijiURI,
      timeRange: TimeRangeSpec,
      timestampField: Option[Symbol],
      inputColumns: Map[String, ColumnInputSpec],
      outputColumns: Map[String, ColumnOutputSpec],
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec)
      extends LocalFijiScheme(
          uri,
          timeRange,
          timestampField,
          inputColumnSpecifyAllData(inputColumns),
          outputColumns,
          rowRangeSpec,
          rowFilterSpec) {
    override def sinkCleanup(
        process: FlowProcess[Properties],
        sinkCall: SinkCall[DirectFijiSinkContext, OutputStream]) {
      // flush table writer
      sinkCall.getContext.writer.flush()
      // Store the output table.
      val conf: JobConf =
        HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))

      // Read table into buffer.
      ResourceUtil.withFijiTable(uri, conf) { table: FijiTable =>
        // We also want the entire time range, so the test can inspect all data in the table.
        val request: FijiDataRequest =
          BaseFijiScheme.buildRequest(table.getLayout, TimeRangeSpec.All, inputColumns.values)

        val overrides: Map[FijiColumnName, ColumnReaderSpec] =
            request
                .getColumns
                .asScala
                .map { column: Column => (column.getColumnName, column.getReaderSpec)}
                .toMap
        val tableReader: FijiTableReader = table.getReaderFactory.readerBuilder()
            .withColumnReaderSpecOverrides(overrides.asJava)
            .buildAndOpen()

        ResourceUtil.doAndClose(tableReader) { reader =>
          // Set up scanning options.
          val eidFactory = EntityIdFactory.getFactory(table.getLayout)
          val scannerOptions = new FijiScannerOptions()
          scannerOptions.setFijiRowFilter(
              rowFilterSpec.toFijiRowFilter.getOrElse(null))
          scannerOptions.setStartRow(
            rowRangeSpec.startEntityId match {
              case Some(entityId) => entityId.toJavaEntityId(eidFactory)
              case None => null
            }
          )
          scannerOptions.setStopRow(
            rowRangeSpec.limitEntityId match {
              case Some(entityId) => entityId.toJavaEntityId(eidFactory)
              case None => null
            }
          )
          ResourceUtil.doAndClose(reader.getScanner(request, scannerOptions)) {
            scanner: FijiRowScanner =>
              scanner.iterator().asScala.foreach { row: FijiRowData =>
                val tuple = FijiScheme.rowToTuple(
                    inputColumns,
                    getSourceFields,
                    timestampField,
                    row)

                val newTupleValues = tuple
                    .iterator()
                    .asScala
                    .map {
                      // This converts stream into a list to force the stream to compute all of the
                      // transformations that have been applied lazily to it. This is necessary
                      // because some of the transformations applied in FijiScheme#rowToTuple have
                      // dependencies on an open connection to a schema table.
                      case stream: Stream[_] => stream.toList
                      case x => x
                    }
                    .toSeq

                buffer.foreach { _ += new Tuple(newTupleValues: _*) }
            }
          }
        }
      }
      super.sinkCleanup(process, sinkCall)
    }
  }

  /**
   * Merges an input column mapping with an output column mapping producing an input column mapping.
   * This is used to configure input columns for reading back written data on a source that has just
   * been used as a sink.
   *
   * @param inputs describing which columns to request and what fields to associate them with.
   * @param outputs describing which columns fields should be output to.
   * @return a merged mapping from field names to input column requests.
   */
  private def mergeColumnMapping(
      inputs: Map[String, ColumnInputSpec],
      outputs: Map[String, ColumnOutputSpec]
  ): Map[String, ColumnInputSpec] = {
    def mergeEntry(
        inputs: Map[String, ColumnInputSpec],
        entry: (String, ColumnOutputSpec)
    ): Map[String, ColumnInputSpec] = {
      val (fieldName, columnRequest) = entry
      val input = ColumnInputSpec(
          column = columnRequest.columnName.getName,
          maxVersions = Int.MaxValue,
          schemaSpec = columnRequest.schemaSpec
      )

      inputs + ((fieldName, input))
    }

    outputs.foldLeft(inputs)(mergeEntry)
  }

  /**
   * A FijiScheme that loads rows in a table into the provided buffer. This class should only be
   * used during tests.
   *
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *     source is used for writing. Specify `None` to write all cells at the current time.
   * @param inputColumns Scalding field name to column input spec mapping.
   * @param outputColumns Scalding field name to column output spec mapping.
   * @param rowRangeSpec is the specification for which interval of rows to scan.
   * @param rowFilterSpec is the specification for which filter to apply.
   */
  private class TestFijiScheme(
      uri: FijiURI,
      timestampField: Option[Symbol],
      inputColumns: Map[String, ColumnInputSpec],
      outputColumns: Map[String, ColumnOutputSpec],
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec)
      extends FijiScheme(
          uri.toString,
          TimeRangeSpec.All,
          timestampField,
          mergeColumnMapping(inputColumns, outputColumns),
          outputColumns,
          rowRangeSpec,
          rowFilterSpec) {
  }
}
