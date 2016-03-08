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

import java.io.IOException
import java.io.ByteArrayOutputStream
import java.io.Closeable

import scala.collection.JavaConversions.asScalaIterator

import org.apache.hadoop.conf.Configuration

import com.moz.fiji.express.avro.generated.ExpressJobHistoryEntry
import com.moz.fiji.schema.avro.RowKeyFormat2
import com.moz.fiji.schema.EntityId
import com.moz.fiji.schema.filter.FormattedEntityIdRowFilter
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiPutter
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiRowScanner
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiDataRequestBuilder
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.express.flow.util.ResourceUtil.doAndClose


/**
 * A class providing an API to install and store job histories for FijiExpress jobs.
 * @param fiji The fiji instance used for the express job.
 */
final class ExpressJobHistoryFijiTable private[express]
  (private[express] val fiji: Fiji)
  extends Closeable {
  import ExpressJobHistoryFijiTable._

  /** Install the express history table if it does not already exist upon instance creation. */
  installIfDoesNotExist()
  /** Open a FijiTable instance for the express job history table.*/
  private val fijiTable: FijiTable =  fiji.openTable(TableName)

  /**
   * Creates a FijiBufferedWriter and calls methods to store the flow and the flow step metrics for
   * the Express job.
   *
   * @param jobId Unique identifier for the job.
   * @param jobName name of the job.
   * @param startTime Time in milliseconds since the epoch at which the job started.
   * @param endTime Time in milliseconds since the epoch at which the job ended.
   * @param jobSuccess Success status of the job.
   * @param counters A map of counter values for the express job.
   * @param conf Configuration of the job.
   * @param extendedInfo A map of additional counters to be stored with the job metrics.
   * @param flowStepCountersInfo An iterable of the maps holding the counter values for each flow
   *    step
   */
  private[express] def recordJob(
      jobId:String,
      jobName:String,
      startTime: Long,
      endTime: Long,
      jobSuccess: Boolean,
      conf: Option[Configuration],
      counters: Map[String, Long],
      extendedInfo: Map[String, String],
      flowStepCountersInfo : Iterable[Map[String, Long]]
  ): Unit = {

    val bufferedPutter: FijiPutter = fijiTable.getWriterFactory.openBufferedWriter()
    try {
      putJobInfo(jobId, jobName, startTime, endTime, jobSuccess, conf, counters,  extendedInfo,
          bufferedPutter)
      putFlowStepInfo(jobId, startTime, flowStepCountersInfo, bufferedPutter)
    } finally {
      bufferedPutter.close()
    }
  }


  /**
   * Writes metrics for the express job flow to the express history table.
   *
   * @param jobId Unique identifier for the job.
   * @param jobName name of the job.
   * @param startTime Time in milliseconds since the epoch at which the job started.
   * @param endTime Time in milliseconds since the epoch at which the job ended.
   * @param jobSuccess Success status of the job.
   * @param counters A map of counter values for the express job.
   * @param conf Configuration of the job.
   * @param extendedInfo A map of additional counters to be stored with the job metrics.
   * @param bufferedPutter A putter instance to store the values in the table.
   */
  private def putJobInfo(
      jobId:String,
      jobName:String,
      startTime: Long,
      endTime: Long,
      jobSuccess: Boolean,
      conf: Option[Configuration],
      counters: Map[String, Long],
      extendedInfo: Map[String, String],
      bufferedPutter: FijiPutter

    ): Unit = {

    //Use the default flowId(0) for storing metrics for the entire flow.
    val entityId: EntityId = fijiTable.getEntityId(jobId, DefaultFlowStepId)
    bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryIdQualifier, startTime, jobId)
    bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryNameQualifier, startTime, jobName)
    bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryStartTimeQualifier, startTime,
        startTime)
    bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryEndTimeQualifier, startTime, endTime)
    bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryCountersQualifier, startTime,
        counters.toString())
    bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryEndStatusQualifier, startTime,
        if (jobSuccess) JobHistorySuccessValue else JobHistoryFailureValue)

    // Put counter values for the job.
    counters.foreach {
      counterVal: (String, Long) => bufferedPutter.put(entityId, JobHistoryCountersFamily,
          counterVal._1, startTime, counterVal._2)
    }

    // Put extended info job counters.
    extendedInfo.foreach {
      extendedInfoVal: (String, String) => bufferedPutter.put(entityId,
          JobHistoryExtendedInfoFamily, extendedInfoVal._1, startTime, extendedInfoVal._2)
    }

    // Store Hadoop Configuration if present.
    conf match {
      case Some(conf) =>
        val byteArrayOutStream: ByteArrayOutputStream = new ByteArrayOutputStream()
        conf.writeXml(byteArrayOutStream)
        bufferedPutter.put(entityId, JobHistoryFamily, JobHistoryConfigurationQualifier,
            startTime, byteArrayOutStream.toString("UTF-8"))
        byteArrayOutStream.close()

      case None => bufferedPutter.put(entityId, JobHistoryFamily,
          JobHistoryConfigurationQualifier, startTime, JobHistoryNoConfigurationValue)
    }
  }


  /**
   * Writes metrics for each of the flow steps to the express job history table.
   *
   * @param jobId Unique identifier for the job.
   * @param startTime Time in milliseconds since the epoch at which the job started.
   * @param flowStepCountersInfo An iterable of the maps holding the counter values for each flow
   *    step
   * @param bufferedPutter A putter instance to store the values in the table.
   */
  private def putFlowStepInfo(
      jobId: String,
      startTime: Long,
      flowStepCountersInfo: Iterable[Map[String, Long]],
      bufferedPutter: FijiPutter
    ): Unit = {
     // Zip the iterable into an indexed list.
    flowStepCountersInfo.zipWithIndex.map { indexedFlowStepMap: (Map[String, Long], Int) =>
      // The (index + 1) in the iterable represents the flow step number.
      val entityId: EntityId = fijiTable.getEntityId(jobId,
          new java.lang.Long(indexedFlowStepMap._2 + 1))
      indexedFlowStepMap._1.map { counter : (String, Long) =>
        bufferedPutter.put(entityId, JobHistoryCountersFamily, counter._1,
            startTime, counter._2)
      }
    }
  }


  /**
   * This method pulls metrics from the express job history table and adds them to an
   * ExpressJobHistoryEntry.Builder instance.
   *
   * @param builder An avro object builder for the ExpressJobHistoryEntry.
   * @param rowScanner A FijiRowScanner object holding rows for the ExpressJobHistoryTable.
   */
  private def buildExpressJobHistoryEntry(
     builder: ExpressJobHistoryEntry.Builder,
     rowScanner: FijiRowScanner
   ) :Unit = {
    rowScanner.iterator().foreach {
      rowData: FijiRowData => {
        //Get the flowStepId from the formatted entityId.
        val jobId : Long = rowData.getEntityId.getComponentByIndex(1)
        jobId match {
          // In case this row holds the job metrics.
          case 0 => {

            builder.setJobId(rowData.getMostRecentValue(
                JobHistoryFamily,
                JobHistoryIdQualifier
            ).toString)
              .setJobName(rowData.getMostRecentValue(
                JobHistoryFamily,
                JobHistoryNameQualifier
            ).toString)
              .setJobStartTime(rowData.getMostRecentValue(
                JobHistoryFamily,
                JobHistoryStartTimeQualifier
            ))

             builder.setJobEndTime(rowData.getMostRecentValue(
                 JobHistoryFamily,
                 JobHistoryEndTimeQualifier
             ))
              .setJobEndStatus(rowData.getMostRecentValue(
                 JobHistoryFamily,
                 JobHistoryEndStatusQualifier
             ).toString)
              .setJobConfiguration(rowData.getMostRecentValue(
                 JobHistoryFamily,
                 JobHistoryConfigurationQualifier
             ).toString)

             builder
              .setJobCounters(rowData.getMostRecentValue(
                  JobHistoryFamily,
                 JobHistoryCountersQualifier
             ).toString)
              .setCountersFamily(rowData.getMostRecentValues(
                 JobHistoryCountersFamily
             ))
              .setExtendedInfo(rowData.getMostRecentValues(
                 JobHistoryExtendedInfoFamily
             ))
          }
          // In case the row holds the flowStep metrics.
          case _=>
            builder.getFlowStepCounters
                .add(rowData.getMostRecentValues(JobHistoryCountersFamily))
        }
      }
    }
  }

  /**
   * This methods retrieves saved metrics for an express job.
   *
   * @param uniqueJobId The uniqueId for the job.
   * @return A ExpressJobHistoryEntry object containing the counter metrics for the requested Job.
   * @throws IOException if there is an IO error while retrieving the data.
   */
  @throws[IOException]
  def getExpressJobDetails(uniqueJobId: String): ExpressJobHistoryEntry = {

    val options: FijiScannerOptions = new FijiScannerOptions
    val fijiDataRequestBuilder : FijiDataRequestBuilder = FijiDataRequest.builder

    fijiDataRequestBuilder
        .newColumnsDef()
        .addFamily(JobHistoryFamily)
        .addFamily(JobHistoryCountersFamily)
        .addFamily(JobHistoryExtendedInfoFamily)

    options.setFijiRowFilter(new FormattedEntityIdRowFilter(
        fijiTable.getLayout.getDesc.getKeysFormat.asInstanceOf[RowKeyFormat2],
        uniqueJobId))

    //Create a builder and initialize the flowStepCounters with an empty list.
    val entryBuilder : ExpressJobHistoryEntry.Builder = ExpressJobHistoryEntry.newBuilder()
        .setFlowStepCounters(new java.util.ArrayList[java.util.Map[String, java.lang.Long]]())

    doAndClose(fijiTable.openTableReader()) { reader =>
      doAndClose(reader.getScanner(fijiDataRequestBuilder.build(), options)) {
        rowScanner => buildExpressJobHistoryEntry(entryBuilder, rowScanner)
      }
    }
    entryBuilder.build()
  }

  /**
   * Called when an instance of ExpressJobHistoryTable class is created. Checks if the express
   * job history table exists in the provided fiji instance. Creates the table if it does not.
   *
   * @return The name of the job history table as used by the installer.
   */
   private def installIfDoesNotExist(): Unit = {
    if (!fiji.getTableNames.contains(TableName)) {
      fiji.createTable(FijiTableLayout
          .createFromEffectiveJsonResource(TableLayoutResource).getDesc())
    }
  }

  override def close(): Unit ={
    fijiTable.release()
  }
}

/**
 * Companion object for ExpressJobHistoryTable.
 */
object ExpressJobHistoryFijiTable {

  /** Name for the express job history table. */
  private val TableName: String = "express_job_history"
  /** Location of the layout json file for the express_job_history table */
  private val TableLayoutResource = "/com.moz.fiji/express/express-job-history-layout.json"

  /** Column families for storing job history info and counters. */
  private val JobHistoryFamily = "info"
  private val JobHistoryCountersFamily = "counters"
  private val JobHistoryExtendedInfoFamily: String = "extendedInfo"

  /** Column Qualifiers for the JobHistoryFamily. */
  private val JobHistoryIdQualifier: String = "jobId"
  private val JobHistoryNameQualifier: String = "jobName"
  private val JobHistoryStartTimeQualifier: String = "startTime"
  private val JobHistoryEndTimeQualifier: String = "endTime"
  private val JobHistoryEndStatusQualifier: String = "jobEndStatus"
  private val JobHistoryCountersQualifier: String = "counters"
  private val JobHistoryConfigurationQualifier: String = "configuration"

  /** Column values for the JobHistoryFamily. */
  private val JobHistoryNoConfigurationValue: String = "No configuration for job."
  private val JobHistorySuccessValue: String = "SUCCEEDED"
  private val JobHistoryFailureValue: String = "FAILED"

  /** FlowStepId that represents metrics for the entire flow in the job history table. */
  private val DefaultFlowStepId: java.lang.Long = 0L

  /**
   * Factory method to create a instance of ExpressJobHistoryFijiTable.
   * @param fiji instance that the express job is run for.
   * @return a new instance of ExpressJobHistoryFijiTable.
   */
  def apply(fiji:Fiji):ExpressJobHistoryFijiTable = new ExpressJobHistoryFijiTable(fiji)

}
