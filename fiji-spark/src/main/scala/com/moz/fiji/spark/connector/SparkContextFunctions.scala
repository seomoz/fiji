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
package com.moz.fiji.spark.connector

import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.spark.connector.rdd.FijiRDD
import org.slf4j.LoggerFactory

/** Provides Fiji-specific methods on `SparkContext` */
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  import SparkContextFunctions._

  /** Returns a view of a Fiji table as `FijiRDD[T]`.
    * This method is made available on `SparkContext` by importing `com.moz.fiji.spark._`
    *
    * @param uri A FijiURI.
    * @param dataRequest A FijiDataRequest.
    * @param vClass ??? Need to talk to Adam.
    * @return An instance of a FijiRDD.
    */
  def fijiRDD[T](uri: FijiURI, dataRequest: FijiDataRequest, vClass: Class[_ <: T]): FijiRDD[T] = {
    val authMode = sc.hadoopConfiguration.get("hbase.security.authentication")
    Log.info(s"Running with $authMode authentication.")

    UserGroupInformation.setConfiguration(sc.hadoopConfiguration)

    val sparkConf = sc.getConf

    val kerberosUsername = sparkConf.getOption("spark.fiji.kerberos.username")
    val keytab = sparkConf.getOption("spark.fiji.kerberos.keytab")

    // If the user specified both properties, then attempt to authenticate
    val ugi = if (kerberosUsername.nonEmpty && keytab.nonEmpty) {
      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        kerberosUsername.get,
        keytab.get
      )
      // Even if we authenticated, only request a token if security is enabled.
      if (UserGroupInformation.isSecurityEnabled) {
        TokenUtil.obtainAndCacheToken(sc.hadoopConfiguration, ugi)
        Log.info("Obtained and cached auth token for HBase.")
      }
      ugi
    } else {
      // Otherwise assume we are either on a non-secure cluster or the HBase auth token
      // has already been cached by the user.
      UserGroupInformation.getCurrentUser
    }

    val credentials = ugi.getCredentials
    FijiRDD(sc, sc.hadoopConfiguration, credentials, uri, dataRequest).asInstanceOf[FijiRDD[T]]
  }
}

object SparkContextFunctions {
  private final val Log = LoggerFactory.getLogger("SparkContextFunctions")
}