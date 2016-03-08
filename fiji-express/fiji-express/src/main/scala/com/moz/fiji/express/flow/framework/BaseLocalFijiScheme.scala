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

import java.io.OutputStream
import java.io.InputStream
import java.util.Properties

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTableReader
import com.moz.fiji.schema.FijiRowScanner
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.layout.ColumnReaderSpec
import com.moz.fiji.schema.FijiDataRequest.Column
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions


/**
 * A Base trait containing Fiji-specific implementation of a Cascading `Scheme` that is common for
 * both the Fields API, and the Type-safe API for running locally. Scheme's [[LocalFijiScheme]] and
 * [[TypedLocalFijiScheme]] extend this trait and share the implemented methods.
 *
 * Note: [[LocalFijiScheme]] and [[TypedLocalFijiScheme]] log every row that was skipped because of
 * missing data in a column. It lacks the parameter `loggingInterval` in [[FijiScheme]] that
 * configures how many skipped rows will be logged.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When FijiScheme is
 * serialized, the result is not persisted anywhere making serialVersionUID unnecessary.
 *
 * Note: If sourcing from a FijiTable, it is never closed.  The reason for this is that if any of
 * the columns in the request are paged, they might still need an open FijiTable for the rest of
 * the flow.  It is expected that any job using this as a source is not long-running and is
 * contained to a single JVM.
 */
private[express] trait BaseLocalFijiScheme
extends Scheme[
      Properties,
      InputStream,
      OutputStream,
      InputContext,
      DirectFijiSinkContext] {

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Fiji table.
   *
   * @param process flow being built.
   * @param tap that is being used with this scheme.
   * @param conf is an unused Properties object that is a stand-in for a job configuration object.
   */
  override def sourceConfInit(
      process: FlowProcess[Properties],
      tap: Tap[Properties, InputStream, OutputStream],
      conf: Properties
  ) {
    // No-op. Setting options in a java Properties object is not going to help us read from
    // a Fiji table.
  }

  /**
   * Cleans up any resources used to read from a Fiji table.
   *
   * Note: This does not close the FijiTable!  If one of the columns of the request was paged,
   * it will potentially still need access to the Fiji table even after the tuples have been
   * sourced.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourceCleanup(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]
  ) {
    // Set the context to null so that we no longer hold any references to it.
    sourceCall.setContext(null)
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Fiji table.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  override def sinkConfInit(
      process: FlowProcess[Properties],
      tap: Tap[Properties, InputStream, OutputStream],
      conf: Properties
  ) {
    // No-op. Setting options in a java Properties object is not going to help us write to
    // a Fiji table.
  }

  /**
   * Cleans up any resources used to write to a Fiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectFijiSinkContext, OutputStream]
  ) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }
}

/**
 * Companion object for the [[BaseLocalFijiScheme]].
 */
object BaseLocalFijiScheme {

  /**
   * Creates a new instances of InputContext that holds the the objects and information required
   * to read the requested data from a fiji table.
   *
   * @param table The Fiji Table being read from.
   * @param conf The Job Configuration to which the data request is added.
   * @param rowFilterSpec The specifications for the row filters for the request.
   * @param rowRangeSpec The specifications for the row range requested.
   * @param request The FijiDataRequest used to read data from the table.
   * @return A InputContext instance that is used to read data from the table.
   */
  def configureInputContext(
      table: FijiTable,
      conf: JobConf,
      rowFilterSpec: RowFilterSpec,
      rowRangeSpec: RowRangeSpec,
      request: FijiDataRequest
  ): InputContext ={

    val overrides: Map[FijiColumnName, ColumnReaderSpec] =
        request
          .getColumns
          .asScala
          .map { column: Column => (column.getColumnName, column.getReaderSpec)}
          .toMap
    val reader: FijiTableReader = table.getReaderFactory.readerBuilder()
        .withColumnReaderSpecOverrides(overrides.asJava)
        .buildAndOpen()

    // Set up the scanner.
    val scanner: FijiRowScanner = if (
      rowFilterSpec.toFijiRowFilter.isEmpty &&
      rowRangeSpec.startEntityId.isEmpty &&
      rowRangeSpec.limitEntityId.isEmpty
    ) {
      // Some implementations of FijiTable (ex. CassandraFijiTable) don't support scanner options at
      // all, so we can't just pass in an empty scanner options.
      reader.getScanner(request)
    } else {
      // Set up scanning options.
      val eidFactory = EntityIdFactory.getFactory(table.getLayout)
      val scannerOptions = new FijiScannerOptions()
      scannerOptions.setFijiRowFilter(rowFilterSpec.toFijiRowFilter.orNull)
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
      reader.getScanner(request, scannerOptions)
    }

    // Store everything in a case class.
    InputContext(
      reader,
      scanner,
      scanner.iterator.asScala,
      table.getURI,
      conf)
  }
}

/**
 * Encapsulates the state required to read from a Fiji table locally, for use in
 * [[com.moz.fiji.express.flow.framework.LocalFijiScheme]].
 *
 * @param reader that has an open connection to the desired Fiji table.
 * @param scanner that has an open connection to the desired Fiji table.
 * @param iterator that maintains the current row pointer.
 * @param tableUri of the fiji table.
 */
@ApiAudience.Private
@ApiStability.Stable
final private[express] case class InputContext(
    reader: FijiTableReader,
    scanner: FijiRowScanner,
    iterator: Iterator[FijiRowData],
    tableUri: FijiURI,
    configuration: Configuration)
