package com.moz.fiji.spark.connector.rdd.cassandra

import java.util.{ArrayList => JArrayList}
import java.util.{List => JList}
import java.util.{SortedMap => JSortedMap}
import java.util.{TreeMap => JTreeMap}
import org.slf4j.LoggerFactory

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import scala.collection.Iterator
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiDataRequest.Column
import com.moz.fiji.schema.FijiResult
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.impl.MaterializedFijiResult
import com.moz.fiji.schema.impl.cassandra.CassandraFiji
import com.moz.fiji.schema.impl.cassandra.CassandraFijiResultScanner
import com.moz.fiji.schema.impl.cassandra.CassandraFijiScannerOptions
import com.moz.fiji.schema.impl.cassandra.CassandraFijiTable
import com.moz.fiji.schema.impl.cassandra.CassandraFijiTableReader
import com.moz.fiji.spark.connector.FijiSpark
import com.moz.fiji.spark.connector.rdd.FijiRDD

/**
 * An RDD that provides the core functionality for reading Fiji data.
 *
 * TODO Add Support for secure clusters
 *
 * Currently, FijiSpark supports only C* and HBase Fiji instances.
 *
 * @param sc The SparkContext to associate this RDD with.
 * @param fijiURI The FijiURI to identify the Fiji instance and table; must include the table name.
 * @param fijiDataRequest The FijiDataRequest for the table provided by fijiURI.
 */
class CassandraFijiRDD[T] private (
    @transient sc: SparkContext,
    @transient fijiURI: FijiURI,
    fijiDataRequest: FijiDataRequest
) extends FijiRDD[T](sc, fijiURI, fijiDataRequest) { //RDD[FijiResult[T]](sc, Nil){
  // TODO (SPARK-34) add the functionality to write Fiji data to a FijiTable from FijiSpark.
  // This would probably consist of adding a method in FijiRDD, e.g. FijiRDD#writeToFijiTable
  // following the style of spark's RDD#write

  import CassandraFijiRDD._
  /**
   * FijiURIs are not serializable; this string representation allows
   * the provided fijiURI to be reconstructed upon deserialization of the RDD.
   */
  private val mFijiURIString = fijiURI.toString

  override def compute(split: Partition, context: TaskContext): Iterator[FijiResult[T]] = {

    val fijiURI: FijiURI = FijiURI.newBuilder(mFijiURIString).build()
    val fiji: Fiji = downcastAndOpenFiji(fijiURI)
    val (reader, scanner) = try {
      val table: FijiTable = downcastAndOpenFijiTable(fiji, fijiURI.getTable)

      try {
        val scannerOptions: CassandraFijiScannerOptions= split match {
//          case hBasePartition: HBaseFijiPartition => {
//            val sOptions = new FijiScannerOptions
//            sOptions.setStartRow(hBasePartition.getStartLocation)
//            sOptions.setStopRow(hBasePartition.getStopLocation)
//            sOptions
//          }
          case cassandraPartition: CassandraFijiPartition =>
            CassandraFijiScannerOptions.withTokens(cassandraPartition.startToken, cassandraPartition.stopToken)

          case _ => throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
        }

        val (reader, scanner) = table.openTableReader() match {
//          case hBaseFijiTableReader: HBaseFijiTableReader => (hBaseFijiTableReader, hBaseFijiTableReader.getFijiResultScanner(fijiDataRequest,
//            scannerOptions.asInstanceOf[FijiScannerOptions]))
          case cassandraFijiTableReader: CassandraFijiTableReader => (cassandraFijiTableReader,
            cassandraFijiTableReader.getScannerWithOptions(fijiDataRequest, scannerOptions.asInstanceOf[CassandraFijiScannerOptions]))
          case _ => throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
        }

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
   // context.addOnCompleteCallback(() => closeResources())
    context.addTaskCompletionListener(context => closeResources())

    // Must return an iterator of MaterializedFijiResults in order to work with the serializer.
    scanner match {
      /**
      case hBaseScanner: HBaseFijiResultScanner[T] => {
        hBaseScanner
          .asScala
          .map({ result: FijiResult[T] =>
          MaterializedFijiResult.create(
            result.getEntityId,
            result.getDataRequest,
            FijiResult.Helpers.getMaterializedContents(result)
          )
        })
      }*/
      case cassandraScanner: CassandraFijiResultScanner[T] => {
        cassandraScanner
            .asScala
            .map{result: FijiResult[T] =>
            MaterializedFijiResult.create(
                result.getEntityId,
                fijiDataRequest,
                FijiResult.Helpers.getMaterializedContents(result)
            )
        }
      }
    }
  }

  /**
   * Helper Method for Cassandra. Converts FijiRowData into materialized contents
   * so that a Materialized FijiResult can be constructed.
   * @param rowData The FijiRowData to be materialized
   * @param dataRequest The dataRequest that produced the fijiRowData
   * @return The materializedContents of the FijiRowData
   */
  private def rowDataMaterialContents(rowData: FijiRowData, dataRequest: FijiDataRequest):
    JSortedMap[FijiColumnName, JList[FijiCell[T]]] = {
      val materializedResult: JSortedMap[FijiColumnName, JList[FijiCell[T]]]  =
          new JTreeMap[FijiColumnName, JList[FijiCell[T]]]
      for(column: Column <- dataRequest.getColumns().asScala) {
        if(column.isPagingEnabled)
            throw new IllegalArgumentException("Columns should not be paged when using FijiSpark")
        val cells: JList[FijiCell[T]] =
            new JArrayList(rowData.getCells[T](column.getFamily, column.getQualifier).values)
        materializedResult.put(column.getColumnName, cells)
      }
      materializedResult
    }

  override protected def getPartitions: Array[Partition] = {
    val fijiURI: FijiURI = FijiURI.newBuilder(mFijiURIString).build()
    if (null == fijiURI.getTable) {
      throw new IllegalArgumentException("FijiURI must specify a table.")
    }
    val fiji: Fiji = downcastAndOpenFiji(fijiURI)
    Log.debug("openedFiji")
    Log.debug("openedFiji")
    try {
      val table: FijiTable = downcastAndOpenFijiTable(fiji, fijiURI.getTable)
      table match {
        /**
        case hBaseTable: HBaseFijiTable => {
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
        } */
        case cassandraTable: CassandraFijiTable => {

          //TODO SPARK-38 Get CassandraFijiPartitions to work correctly(just return one partition for now)
          //          val partitions = getCassandraPartitions((Long.MinValue :: getTokens(getSession)) :+ Long.MaxValue)
          val partitions = new Array[Partition](1)
          partitions(0) = CassandraFijiPartition(0, Long.MinValue, Long.MaxValue)
          table.release()
          partitions
        }
      }
    } finally {
      fiji.release()
    }
  }

  /**
   * Opens and returns the Fiji instance
   *
   *
   * @param fijiURI the FijiURI specifying the instance and table.
   * @return Fiji instance specified by fijiURI.
   * @throws UnsupportedOperationException if the Fiji is not C* or HBase.
   */
  private def downcastAndOpenFiji(fijiURI: FijiURI): Fiji = {
    Log.debug("opening fiji")
    Fiji.Factory.open(fijiURI) match {
      //case hbaseFiji: HBaseFiji => hbaseFiji
      case cassandraFiji: CassandraFiji => cassandraFiji
      case nonHBaseFiji: Fiji => {
        nonHBaseFiji.release()
        throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
      }
    }
  }

  /**
   * Opens and returns the FijiTable.
   *
   * @param fiji the fiji instance containing the table.
   * @param tableName the name of the table to open and downcast.
   * @return the FijiTable specified by tableName.
   * @throws UnsupportedOperationException if the FijiTable is not an C* or HBase.
   */
  private def downcastAndOpenFijiTable(fiji: Fiji, tableName: String): FijiTable = {
    fiji.openTable(tableName) match {
      //case hBaseFijiTable: HBaseFijiTable => hBaseFijiTable
      case cassandraFijiTable: CassandraFijiTable => cassandraFijiTable
      case nonHBaseFijiTable: FijiTable => {
        nonHBaseFijiTable.release()
        throw new UnsupportedOperationException(FijiSpark.UnsupportedFiji)
      }
    }
  }

  //TODO SPARK-38 Attempt to make Cassandra partitions work. There are still some bugs. Attempted to copy
  //TODO the getInputSplits in CassandraMapReduce

  //  private def getTokens(session: Session): List[Long] = {
  //    def tokens(queryString: String) = {
  //      val resultSet: ResultSet = session.execute(queryString)
  //      val results: java.util.List[Row] = resultSet.all
  //      results
  //    }
  //    val localResults = tokens("SELECT tokens FROM system.local;")
  //    Preconditions.checkArgument(localResults.size == 1)
  //    val localTokens: List[Long] = localResults.get(0).getSet("tokens", classOf[String]).asScala.toList.map(_.toLong)
  //    var allTokens = localTokens
  //    val peerResults: List[Row] = tokens("SELECT rpc_address, tokens FROM system.peers;").asScala.iterator.toList
  //    for(row <- peerResults) {
  //      val tokens = row.getSet("tokens", classOf[String]).asScala
  //      val rpcAddress: InetAddress = row.getInet("rpc_address")
  //      val hostName: String = rpcAddress.getHostName
  //      Preconditions.checkArgument(!(hostName == "localhost"))
  //      allTokens = allTokens ::: tokens.toList.map(_.toLong)
  //    }
  //    allTokens
  //  }
  //
  //  private def getCassandraPartitions(tokens: List[Long]): Array[Partition] = {
  //    val sortedTokens = tokens.sorted
  //    val partitions = new Array[Partition](tokens.size)
  //    for (i <- 0 until (tokens.size - 1)) {
  //      val startToken = if (i > 0) sortedTokens(i) + 1 else sortedTokens(i)
  //      val endToken = sortedTokens(i + 1)
  //      partitions(i) = new CassandraFijiPartition(i, startToken, endToken)
  //    }
  //    partitions
  //
  //  }
  //    private def getSession(): Session = {
  //      val cluster: Cluster = Cluster
  //          .builder
  //          .addContactPoints(fijiURI.asInstanceOf[CassandraFijiURI].getContactPoints
  //          .toArray(new Array[String](fijiURI.asInstanceOf[CassandraFijiURI].getContactPoints.size)): _*)
  //          .withPort(fijiURI.asInstanceOf[CassandraFijiURI].getContactPort)
  //          .build
  //      cluster.connect
  //    }

}

/** Companion object containing static members used by the FijiRDD class. */
object CassandraFijiRDD {
  private final val Log = LoggerFactory.getLogger(classOf[CassandraFijiRDD[_]])

  /**
   *
   * @param sc
   * @param fijiURI
   * @param fijiDataRequest
   * @return
   */
  def apply(
      @transient sc: SparkContext,
      @transient fijiURI: FijiURI,
      fijiDataRequest: FijiDataRequest
  ): CassandraFijiRDD[_] = {
    new CassandraFijiRDD(sc, fijiURI, fijiDataRequest)
  }
}
