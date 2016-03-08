package com.moz.fiji.spark.connector

/**
 * Defines static global variables
 */
object FijiSpark {

  /** Error message indicating that the Fiji instance must be an HBaseFiji or a CassandraFiji. */
  val UnsupportedFiji = "FijiSpark currently only supports HBase and Cassandra Fiji instances."
  val IncorrectHBaseParams = "Error: You passed in parameters for an HBase table but specified a Cassandra table"
  val IncorrectCassandraParams = "Error: You passed in parameters for a Cassandra table but specified and HBase table"
  val HbaseFiji = "hbaseFiji"
  val CassandraFiji = "cassandraFiji"
}
