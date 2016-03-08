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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import java.util.UUID

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import cascading.tuple.TupleEntrySchemeCollector
import cascading.tuple.TupleEntrySchemeIterator
import com.google.common.base.Objects
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.util.ResourceUtil.doAndRelease
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiURI

/**
 * A Fiji-specific implementation of a Cascading `Tap`, which defines the location of a Fiji table.
 *
 * LocalFijiTap is responsible for configuring a local Cascading job with the settings necessary to
 * read from a Fiji table.
 *
 * LocalFijiTap must be used with [[com.moz.fiji.express.flow.framework.LocalFijiScheme]] to perform
 * decoding of cells in a Fiji table. [[com.moz.fiji.express.flow.FijiSource]] handles the creation
 * of both LocalFijiScheme and LocalFijiTap in FijiExpress.
 *
 * @param uri of the Fiji table to read or write from.
 * @param scheme that will convert data read from Fiji into Cascading's tuple model.
 */
@ApiAudience.Framework
@ApiStability.Stable
final private[express] class LocalFijiTap(
    uri: FijiURI,
    private val scheme: BaseLocalFijiScheme)
    extends Tap[Properties, InputStream, OutputStream](
        scheme.asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]) {

  /** The URI of the table to be read through this tap. */
  private[express] val tableUri: String = uri.toString

  /** A unique identifier for this tap instance. */
  private val id: String = UUID.randomUUID().toString

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Fiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sourceConfInit(
      flow: FlowProcess[Properties],
      conf: Properties) {
    // Store the input table.
    conf.setProperty(FijiConfKeys.KIJI_INPUT_TABLE_URI, tableUri)

    super.sourceConfInit(flow, conf)
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Fiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sinkConfInit(
      flow: FlowProcess[Properties],
      conf: Properties) {
    // Store the output table.
    conf.setProperty(FijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri)

    super.sinkConfInit(flow, conf)
  }

  /**
   * Provides a string representing the resource this `Tap` instance represents.
   *
   * @return a java UUID representing this FijiTap instance. Note: This does not return the uri of
   *     the Fiji table being used by this tap to allow jobs that read from or write to the same
   *     table to have different data request options.
   */
  override def getIdentifier: String = id

  /**
   * Opens any resources required to read from a Fiji table.
   *
   * @param flow being run.
   * @param input stream that will read from the desired Fiji table.
   * @return an iterator that reads rows from the desired Fiji table.
   */
  override def openForRead(
      flow: FlowProcess[Properties],
      input: InputStream): TupleEntryIterator =
    new TupleEntrySchemeIterator[Properties, InputStream](
        flow,
        getScheme,
        if (null == input) new ByteArrayInputStream(Array()) else input,
        getIdentifier())

  /**
   * Opens any resources required to write from a Fiji table.
   *
   * @param flow being run.
   * @param output stream that will write to the desired Fiji table. Note: This is ignored
   *     currently since writing to a Fiji table is currently implemented without using an output
   *     format by writing to the table directly from
   *     [[com.moz.fiji.express.flow.framework.FijiScheme]].
   * @return a collector that writes tuples to the desired Fiji table.
   */
  override def openForWrite(
      flow: FlowProcess[Properties],
      output: OutputStream): TupleEntryCollector =
    new TupleEntrySchemeCollector[Properties, OutputStream](
        flow,
        getScheme,
        if (null == output) new ByteArrayOutputStream() else output,
        getIdentifier())

  /**
   * Builds any resources required to read from or write to a Fiji table.
   *
   * Note: FijiExpress currently does not support automatic creation of Fiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if required resources were created successfully.
   * @throws UnsupportedOperationException always.
   */
  override def createResource(conf: Properties): Boolean =
    throw new UnsupportedOperationException("FijiTap does not support creating tables for you.")

  /**
   * Deletes any unnecessary resources used to read from or write to a Fiji table.
   *
   * Note: FijiExpress currently does not support automatic deletion of Fiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if superfluous resources were deleted successfully.
   * @throws UnsupportedOperationException always.
   */
  override def deleteResource(conf: Properties): Boolean =
    throw new UnsupportedOperationException("FijiTap does not support deleting tables for you.")

  /**
   * Determines if the Fiji table this `Tap` instance points to exists.
   *
   * @param conf containing settings for this flow.
   * @return true if the target Fiji table exists.
   */
  override def resourceExists(conf: Properties): Boolean = {
    val jobConf: JobConf = HadoopUtil.createJobConf(conf,
        new JobConf(HBaseConfiguration.create()))
    doAndRelease(Fiji.Factory.open(uri, jobConf)) { fiji: Fiji =>
      fiji.getTableNames.contains(uri.getTable)
    }
  }

  /**
   * Gets the time that the target Fiji table was last modified.
   *
   * Note: This will always return the current timestamp.
   *
   * @param conf containing settings for this flow.
   * @return the current time.
   */
  override def getModifiedTime(conf: Properties): Long = System.currentTimeMillis()

  override def equals(obj: Any): Boolean = obj match {
    case other: LocalFijiTap => (tableUri == other.tableUri
        && scheme == other.scheme
        && id == other.id)
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)

  /**
   * Checks whether the instance, tables, and columns this tap uses can be accessed.
   *
   * @throws FijiExpressValidationException if the tables and columns are not accessible when this
   *    is called.
   */
  private[express] def validate(conf: Properties): Unit =
    scheme match {
      case localFijiScheme: LocalFijiScheme =>
        FijiTap.validate(
            uri,
            localFijiScheme.inputColumns.values.toList,
            localFijiScheme.outputColumns.values.toList,
            HadoopUtil.createJobConf(conf, new JobConf(HBaseConfiguration.create())))
      case typedLocalFijiScheme: TypedLocalFijiScheme =>
        FijiTap.validate(
            uri,
            typedLocalFijiScheme.inputColumns,
            // TypedLocalFijiScheme takes no output column params.
            Seq(),
            HadoopUtil.createJobConf(conf, new JobConf(HBaseConfiguration.create())))
    }
}
