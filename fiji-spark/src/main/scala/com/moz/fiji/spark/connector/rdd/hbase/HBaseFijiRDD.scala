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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.moz.fiji.spark.connector.rdd.hbase

import scala.collection.Iterator
import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark.Partition
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext

import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTableReader
import com.moz.fiji.schema.FijiResult
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.hbase.HBaseFijiURI
import com.moz.fiji.schema.impl.MaterializedFijiResult
import com.moz.fiji.schema.impl.hbase.HBaseFiji
import com.moz.fiji.schema.impl.hbase.HBaseFijiResultScanner
import com.moz.fiji.schema.impl.hbase.HBaseFijiTable
import com.moz.fiji.schema.impl.hbase.HBaseFijiTableReader
import com.moz.fiji.spark.connector.FijiSpark
import com.moz.fiji.spark.connector.rdd.FijiRDD

/**
 * An RDD that provides the core functionality for reading Fiji data.
 *
 * Currently, FijiSpark supports only HBase Fiji instances.
 *
 * @param sc The SparkContext to associate this RDD with.
 * @param fijiURI The FijiURI to identify the Fiji instance and table; must include the table name.
 * @param fijiDataRequest The FijiDataRequest for the table provided by fijiURI.
 */
class HBaseFijiRDD[T] (
    @transient sc: SparkContext,
    @transient conf: Configuration,
    @transient credentials: Credentials,
    @transient fijiURI: FijiURI,
    fijiDataRequest: FijiDataRequest
) extends FijiRDD[T](sc, /*conf, credentials,*/ fijiURI, fijiDataRequest) {

  /**
   * FijiURIs are not serializable; this string representation allows
   * the provided fijiURI to be reconstructed upon deserialization of the RDD.
   */
  private val mFijiURIString = fijiURI.toString
  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))
  private val credentialsBroadcast = sc.broadcast(new SerializableWritable(credentials))

  override def compute(split: Partition, context: TaskContext): Iterator[FijiResult[T]] = {
    val ugi = UserGroupInformation.getCurrentUser
    ugi.addCredentials(credentialsBroadcast.value.value)
    ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

    val partition = split.asInstanceOf[HBaseFijiPartition]

    val fijiURI = HBaseFijiURI.newBuilder(mFijiURIString).build()
    val fiji: HBaseFiji = downcastAndOpenHBaseFiji(fijiURI)

    val (reader, scanner) = try {
      val table: HBaseFijiTable = downcastAndOpenHBaseFijiTable(fiji, fijiURI.getTable)

      try {
        // Reader must be HBaseFijiTableReader in order to return a FijiResultScanner.
        val reader: HBaseFijiTableReader = table.openTableReader() match {
          case hBaseFijiTableReader: HBaseFijiTableReader => hBaseFijiTableReader
          case _ => throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
        }
        val scannerOptions: FijiTableReader.FijiScannerOptions = new FijiScannerOptions
        scannerOptions.setStartRow(partition.startLocation)
        scannerOptions.setStopRow(partition.stopLocation)
        val scanner: HBaseFijiResultScanner[T] =
            reader.getFijiResultScanner(fijiDataRequest, scannerOptions)

        (reader, scanner)

      } finally {
        table.release()
      }
    } finally {
      fiji.release()
    }

    def closeResources() {
      scanner.close()
      reader.close()
    }

    // Register an on-task-completion callback to close the input stream.
    //context.addOnCompleteCallback(() => closeResources())
    context.addTaskCompletionListener(context => closeResources())

    // Must return an iterator of MaterializedFijiResults in order to work with the serializer.
    scanner
        .asScala
        .map{ result: FijiResult[T] =>
        MaterializedFijiResult.create(
            result.getEntityId,
            result.getDataRequest,
            FijiResult.Helpers.getMaterializedContents(result)
        )
    }
  }

  override def checkpoint(): Unit = super.checkpoint()

  override protected def getPartitions: Array[Partition] = {
    val ugi = UserGroupInformation.getCurrentUser
    ugi.addCredentials(credentialsBroadcast.value.value)
    ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

    val fijiURI = HBaseFijiURI.newBuilder(mFijiURIString).build()
    if (null == fijiURI.getTable) {
      throw new IllegalArgumentException("FijiURI must specify a table.")
    }
    val fiji: HBaseFiji = downcastAndOpenHBaseFiji(fijiURI)

    try {
      val table: HBaseFijiTable = downcastAndOpenHBaseFijiTable(fiji, fijiURI.getTable)
      val regions = table.getRegions
      val numRegions = regions.size()
      val result = new Array[Partition](numRegions)

      for (i <- 0 until numRegions) {
        val startKey: Array[Byte] = regions.get(i).getStartKey
        val endKey: Array[Byte] = regions.get(i).getEndKey
        result(i) = HBaseFijiPartition(i, startKey, endKey)
      }

      table.release()
      result
    } finally {
      fiji.release()
    }
  }

  /**
   * Opens and returns the Fiji instance; throws an exception if it is not an HBaseFiji.
   *
   * @param fijiURI the FijiURI specifying the instance and table.
   * @return HBaseFiji instance specified by fijiURI.
   * @throws UnsupportedOperationException if the Fiji is not an HBaseFiji.
   */
  private def downcastAndOpenHBaseFiji(fijiURI: FijiURI): HBaseFiji = {
    Fiji.Factory.open(fijiURI, confBroadcast.value.value) match {
      case fiji: HBaseFiji => fiji
      case nonHBaseFiji: Fiji =>
        nonHBaseFiji.release()
        throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
    }
  }

  /**
   * Opens and returns the FijiTable; throws an exception if it is not an HBaseFijiTable.
   *
   * @param fiji the fiji instance containing the table.
   * @param tableName the name of the table to open and downcast.
   * @return the HBaseFijiTable specified by tableName.
   * @throws UnsupportedOperationException if the FijiTable is not an HBaseFijiTable.
   */
  private def downcastAndOpenHBaseFijiTable(fiji: HBaseFiji, tableName: String): HBaseFijiTable = {
    fiji.openTable(tableName) match {
      case hBaseFijiTable: HBaseFijiTable => hBaseFijiTable
      case nonHBaseFijiTable: FijiTable =>
        nonHBaseFijiTable.release()
        throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
    }
  }
}

/** Companion object containing static members used by the FijiRDD class. */
object HBaseFijiRDD {

  def apply(
      @transient sc: SparkContext,
      @transient conf: Configuration,
      @transient credentials: Credentials,
      @transient fijiURI: FijiURI,
      fijiDataRequest: FijiDataRequest
  ): HBaseFijiRDD[_] = {
    new HBaseFijiRDD(sc, conf, credentials, fijiURI, fijiDataRequest)
  }
}