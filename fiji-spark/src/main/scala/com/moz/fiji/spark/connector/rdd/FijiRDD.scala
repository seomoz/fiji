package com.moz.fiji.spark.connector.rdd

import scala.collection.Iterator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.moz.fiji.schema.cassandra.CassandraFijiURI
import com.moz.fiji.schema.hbase.HBaseFijiURI
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.FijiResult
import com.moz.fiji.spark.connector.rdd.hbase.HBaseFijiRDD
import com.moz.fiji.spark.connector.rdd.cassandra.CassandraFijiRDD
import com.moz.fiji.spark.connector.FijiSpark

/**
 *
 */
abstract class FijiRDD[T] private[rdd] (
    @transient sc: SparkContext,
    //@transient conf: Configuration = None, TODO: add support
    //@transient credentials: Credentials = None,
    @transient fijiURI: FijiURI,
    fijiDataRequest: FijiDataRequest
) extends RDD[FijiResult[T]](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[FijiResult[T]]
  override protected def getPartitions: Array[Partition]
}

object FijiRDD {

  /**
   *
   * @param sc
   * @param conf
   * @param credentials
   * @param fijiURI
   * @param fijiDataRequest
   * @return
   */
  def apply (
      @transient sc: SparkContext,
      @transient fijiURI: FijiURI,
      fijiDataRequest: FijiDataRequest
  ): FijiRDD[_] = {
    fijiURI match {
      case cassandraFijiURI: CassandraFijiURI => CassandraFijiRDD(sc, fijiURI, fijiDataRequest)
      case _ => throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
    }
  }

  /**
   *
   * @param sc
   * @param conf
   * @param credentials
   * @param fijiURI
   * @param fijiDataRequest
   * @return
   */
  def apply (
      @transient sc: SparkContext,
      @transient conf: Configuration,
      @transient credentials: Credentials,
      @transient fijiURI: FijiURI,
      fijiDataRequest: FijiDataRequest
  ): FijiRDD[_] = {
    fijiURI match {
      case hbaseFijiURI: HBaseFijiURI => HBaseFijiRDD(sc, conf, credentials, fijiURI, fijiDataRequest)
      case cassandraFijiURI: CassandraFijiURI => CassandraFijiRDD(sc, fijiURI, fijiDataRequest)
      case _ => throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
    }
  }
}
