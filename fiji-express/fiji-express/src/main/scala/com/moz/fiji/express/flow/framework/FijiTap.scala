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

import java.util.UUID

import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.lib.NullOutputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.token.TokenIdentifier
import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.ColumnOutputSpec
import com.moz.fiji.express.flow.InvalidFijiTapException
import com.moz.fiji.express.flow.util.ResourceUtil.doAndRelease
import com.moz.fiji.mapreduce.framework.FijiConfKeys
import com.moz.fiji.mapreduce.framework.FijiTableInputFormat
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.{EntityId => JEntityId}
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * A Fiji-specific implementation of a Cascading `Tap`, which defines the location of a Fiji table.
 *
 * FijiTap is responsible for configuring a MapReduce job with the correct input format for reading
 * from a Fiji table.
 *
 * FijiTap must be used with [[com.moz.fiji.express.flow.framework.FijiScheme]] to perform decoding of
 * cells in a Fiji table. [[com.moz.fiji.express.flow.FijiSource]] handles the creation of both
 * FijiScheme and FijiTap in FijiExpress.
 *
 * @param uri of the Fiji table to read or write from.
 * @param scheme that will convert data read from Fiji into Cascading's tuple model.
 */
@ApiAudience.Framework
@ApiStability.Stable
final class FijiTap(
    // This is not a val because FijiTap needs to be serializable and FijiURI is not.
    uri: FijiURI,
    private val scheme: BaseFijiScheme
) extends Tap[
    JobConf,
    RecordReader[Container[JEntityId], Container[FijiRowData]],
    OutputCollector[_, _]
](
    scheme.asInstanceOf[
        Scheme[
            JobConf,
            RecordReader[Container[JEntityId], Container[FijiRowData]],
            OutputCollector[_, _],
            _,
            _
        ]
    ]
) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[FijiTap])

  /** Address of the table to read from or write to. */
  private[express] val tableUri: String = uri.toString

  /** Unique identifier for this FijiTap instance. */
  private val id: String = UUID.randomUUID().toString

  /**
   * Get the tokens from the current user, and add them to the credentials in the jobConf.  This
   * is only necessary for operating on a secure cluster, where the HBase delegation tokens are
   * needed to communicate with HBase.
   *
   * This is safe to run on non-secure clusters, because there will be no tokens for the current
   * user and the jobConf will not be modified.
   *
   * @param jobConf to add the tokens to.
   */
  def initializeTokens(jobConf: JobConf): Unit = {
    if (User.isHBaseSecurityEnabled(jobConf)) {
      val user = UserGroupInformation.getCurrentUser
      val tokens = user.getTokens.iterator.asScala.toSeq
      val credentials = jobConf.getCredentials
      tokens.foreach { token: Token[_ <: TokenIdentifier] =>
        logger.debug("Adding token %s for user %s to JobConf credentials.".format(token, user))
        credentials.addToken(token.getKind, token)
      }
    }
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Fiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sourceConfInit(flow: FlowProcess[JobConf], conf: JobConf) {
    // Configure the job's input format.
    val uri: FijiURI = FijiURI.newBuilder(tableUri).build()
    val inputFormat: FijiTableInputFormat = FijiTableInputFormat.Factory.get(uri).getInputFormat
    MapredInputFormatWrapper.setInputFormat(inputFormat.getClass, conf)

    // Store the input table.
    conf.set(FijiConfKeys.FIJI_INPUT_TABLE_URI, tableUri)

    initializeTokens(conf)
    super.sourceConfInit(flow, conf)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Fiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sinkConfInit(flow: FlowProcess[JobConf], conf: JobConf) {
    // Configure the job's output format.
    conf.setOutputFormat(classOf[NullOutputFormat[_, _]])

    // Store the output table.
    conf.set(FijiConfKeys.FIJI_OUTPUT_TABLE_URI, tableUri)

    initializeTokens(conf)
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
   * @param recordReader that will read from the desired Fiji table.
   * @return an iterator that reads rows from the desired Fiji table.
   */
  override def openForRead(
      flow: FlowProcess[JobConf],
      recordReader: RecordReader[Container[JEntityId], Container[FijiRowData]]
  ): TupleEntryIterator = {
    val modifiedFlow = if (flow.getStringProperty(FijiConfKeys.FIJI_INPUT_TABLE_URI) == null) {
      // TODO CHOP-71 Remove this hack which is introduced by a scalding bug:
      // https://github.com/twitter/scalding/issues/369
      // This hack is only required for testing (HadoopTest Mode)
      val jconf = flow.getConfigCopy
      val fp = new HadoopFlowProcess(jconf)
      sourceConfInit(fp, jconf)
      fp
    } else {
      flow
    }
    new HadoopTupleEntrySchemeIterator(
        modifiedFlow,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        recordReader)
  }

  /**
   * Opens any resources required to write from a Fiji table.
   *
   * @param flow being run.
   * @param outputCollector that will write to the desired Fiji table. Note: This is ignored
   *     currently since writing to a Fiji table is currently implemented without using an output
   *     format by writing to the table directly from
   *     [[com.moz.fiji.express.flow.framework.FijiScheme]].
   * @return a collector that writes tuples to the desired Fiji table.
   */
  override def openForWrite(
      flow: FlowProcess[JobConf],
      outputCollector: OutputCollector[_, _]
  ): TupleEntryCollector = {
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
  override def createResource(conf: JobConf): Boolean = {
    throw new UnsupportedOperationException("FijiTap does not support creating tables for you.")
  }

  /**
   * Deletes any unnecessary resources used to read from or write to a Fiji table.
   *
   * Note: FijiExpress currently does not support automatic deletion of Fiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if superfluous resources were deleted successfully.
   * @throws UnsupportedOperationException always.
   */
  override def deleteResource(conf: JobConf): Boolean = {
    throw new UnsupportedOperationException("FijiTap does not support deleting tables for you.")
  }

  /**
   * Determines if the Fiji table this `Tap` instance points to exists.
   *
   * @param conf containing settings for this flow.
   * @return true if the target Fiji table exists.
   */
  override def resourceExists(conf: JobConf): Boolean = {
    val uri: FijiURI = FijiURI.newBuilder(tableUri).build()

    doAndRelease(Fiji.Factory.open(uri, conf)) { fiji: Fiji =>
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
  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()

  override def equals(other: Any): Boolean = {
    other match {
      case tap: FijiTap => (tableUri == tap.tableUri) && (scheme == tap.scheme) && (id == tap.id)
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)

  /**
   * Checks whether the instance, tables, and columns this tap uses can be accessed.
   *
   * @throws FijiExpressValidationException if the tables and columns are not accessible when this
   *    is called.
   */
  private[express] def validate(conf: Configuration): Unit = {
    val fijiUri: FijiURI = FijiURI.newBuilder(tableUri).build()
    scheme match {
      case fijiScheme: FijiScheme =>  FijiTap.validate (
          fijiUri,
          fijiScheme.inputColumns.values.toList,
          fijiScheme.outputColumns.values.toList,
          conf
      )
      case fijiTypedScheme: TypedFijiScheme => FijiTap.validate(
          fijiUri,
          fijiTypedScheme.inputColumns,
          //FijiTypedSource takes no output column params.
          Seq(),
          conf
      )
    }
  }
}

@ApiAudience.Framework
@ApiStability.Stable
object FijiTap {
  /**
   * Checks whether the instance, tables, and columns specified can be accessed.
   *
   * @throws FijiExpressValidationException if the tables and columns are not accessible when this
   *    is called.
   */
  private[express] def validate(
      fijiUri: FijiURI,
      inputColumns: Seq[ColumnInputSpec],
      outputColumns: Seq[ColumnOutputSpec],
      conf: Configuration
  ) {
    // Try to open the Fiji instance.
    val fiji: Fiji =
        try {
          Fiji.Factory.open(fijiUri, conf)
        } catch {
          case e: Exception =>
            throw new InvalidFijiTapException(
                "Error opening Fiji instance: %s\n".format(fijiUri.getInstance()), e)
        }

    // Try to open the table.
    val table: FijiTable =
        try {
          fiji.openTable(fijiUri.getTable)
        } catch {
          case e: Exception =>
            throw new InvalidFijiTapException(
                "Error opening Fiji table: %s\n".format(fijiUri.getTable) + e.getMessage)
        } finally {
          fiji.release() // Release the Fiji instance.
        }

    // Check the columns are valid
    val tableLayout: FijiTableLayout = table.getLayout
    table.release() // Release the FijiTable.

    // Get a list of columns that don't exist
    val inputColumnNames: Seq[FijiColumnName] = inputColumns.map(_.columnName).toList
    val outputColumnNames: Seq[FijiColumnName] = outputColumns.map(_.columnName).toList

    val nonExistentColumnErrors = (inputColumnNames ++ outputColumnNames)
        // Filter for illegal columns, so we can throw an error.
        .filter( { case colname => {
            if (tableLayout.exists(colname)) {
              // If colname exists in the table layout as a qualified column, then it's legal.
              false
            } else if (tableLayout.getFamilyMap.containsKey(colname.getFamily)) {
              // If colname.getFamily is in the families in the layout,
              // AND that family is a map-type family, then it's legal.
              if (tableLayout.getFamilyMap.get(colname.getFamily).isMapType) {
                false
              } else {
                // If colname.getFamily is not a map-type family, then it's illegal anyways.
                true
              }
            } else {
              // If colname.getFamily is not even in the table layout then it's definitely illegal.
              true
            } } } )
        .map { column =>
          "One or more columns does not exist in the table %s: %s\n".format(table.getName, column)
        }

    val allErrors = nonExistentColumnErrors

    // Combine all error strings.
    if (!allErrors.isEmpty) {
      throw new InvalidFijiTapException(
          "Errors found in validating Tap: %s".format(allErrors.mkString(", \n"))
      )
    }
  }

}
