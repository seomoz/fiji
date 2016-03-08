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

import java.util.UUID

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.framework.FijiTap
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.mapreduce.impl.HFileWriterContext
import com.moz.fiji.mapreduce.output.framework.FijiHFileOutputFormat
import com.moz.fiji.schema.FijiURI

/**
 * A Fiji-specific implementation of a Cascading `Tap`, which defines how data is to be read from
 * and written to a particular endpoint. This implementation only handles writing to Fiji
 * formatted HFiles to be bulk loaded into HBase.
 *
 * HFileFijiTap must be used with [[com.moz.fiji.express.flow.framework.hfile.HFileFijiScheme]]
 * to perform decoding of cells in a Fiji table. [[com.moz.fiji.express.flow.FijiSource]] handles
 * the creation of both HFileFijiScheme and HFileFijiTap in FijiExpress.
 *
 * @param tableUri of the Fiji table to read or write from.
 * @param scheme that will convert data read from Fiji into Cascading's tuple model.
 * @param hFileOutput is the location where the HFiles will be written to.
 */
@ApiAudience.Framework
@ApiStability.Stable
final private[express] class HFileFijiTap(
  private[express] val tableUri: String,
  private[express] val scheme: HFileFijiScheme,
  private[express] val hFileOutput: String)
    extends Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]](
        scheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]) {

  /** Unique identifier for this FijiTap instance. */
  private val id: String = UUID.randomUUID().toString

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
   * @param recordReader that will read from the desired Fiji table.
   * @return an iterator that reads rows from the desired Fiji table.
   */
  override def openForRead(
      flow: FlowProcess[JobConf],
      recordReader: RecordReader[_, _]): TupleEntryIterator = {
    null
  }

  /**
   * Opens any resources required to write from a Fiji table.
   *
   * @param flow being run.
   * @param outputCollector that will write to the desired Fiji table.
   *
   * @return a collector that writes tuples to the desired Fiji table.
   */
  override def openForWrite(
      flow: FlowProcess[JobConf],
      outputCollector: OutputCollector[_, _]): TupleEntryCollector = {

    new HadoopTupleEntrySchemeCollector(
        flow,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        outputCollector)
  }

  /**
   * Builds any resources required to read from or write to a Fiji table.
   *
   * Note: FijiExpress currently does not support automatic creation of Fiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if required resources were created successfully.
   * @throws UnsupportedOperationException always.
   */
  override def createResource(conf: JobConf): Boolean =
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
  override def deleteResource(conf: JobConf): Boolean =
    throw new UnsupportedOperationException("FijiTap does not support deleting tables for you.")

  /**
   * Gets the time that the target Fiji table was last modified.
   *
   * Note: This will always return the current timestamp.
   *
   * @param conf containing settings for this flow.
   * @return the current time.
   */
  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()

  /**
   * Determines if the Fiji table this `Tap` instance points to exists.
   *
   * @param conf containing settings for this flow.
   * @return true if the target Fiji table exists.
   */
  override def resourceExists(conf: JobConf): Boolean = true

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Fiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sinkConfInit(flow: FlowProcess[JobConf], conf: JobConf) {
    FileOutputFormat.setOutputPath(conf, new Path(hFileOutput, "hfiles"))
    DeprecatedOutputFormatWrapper.setOutputFormat(classOf[FijiHFileOutputFormat], conf)
    val hfContext = classOf[HFileWriterContext].getName
    conf.set(FijiConfKeys.KIJI_TABLE_CONTEXT_CLASS, hfContext)
    // Store the output table.
    conf.set(FijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri)

    super.sinkConfInit(flow, conf)
  }

  /**
   * Checks whether the instance, tables, and columns this tap uses can be accessed.
   *
   * @throws FijiExpressValidationException if the tables and columns are not accessible when this
   *    is called.
   */
  private[express] def validate(conf: Configuration): Unit = {
    val fijiUri: FijiURI = FijiURI.newBuilder(tableUri).build()
    FijiTap.validate(fijiUri, Seq(), scheme.outputColumns.values.toList, conf)
  }
}
