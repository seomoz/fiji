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

package com.moz.fiji.schema.shell.util

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.hbase.HBaseConfiguration

import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiInstaller
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.util.ProtocolVersion
import com.moz.fiji.schema.shell.input.NullInputSource

import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.FijiSystem
import com.moz.fiji.schema.shell.AbstractFijiSystem

trait FijiIntegrationTestHelpers {

  def getFijiSystem(): AbstractFijiSystem = {
    return new FijiSystem
  }

  private val mNextInstanceId = new AtomicInteger(0)

  /**
   * @return the name of a unique Fiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): FijiURI = {
    val id = mNextInstanceId.incrementAndGet()
    val uri = FijiURI.newBuilder().withZookeeperQuorum(Array(".fake." +
      id)).withInstanceName(getClass().getName().replace(".", "_")).build()
    installFiji(uri)
    return uri
  }

  /**
   * Install a Fiji instance.
   */
  def installFiji(instanceURI: FijiURI): Unit = {
    FijiInstaller.get().install(instanceURI, HBaseConfiguration.create())

    // This requires a system-2.0-based Fiji. Explicitly set it before we create
    // any tables, if it's currently on system-1.0.
    val fiji: Fiji = Fiji.Factory.open(instanceURI)
    try {
      val curDataVersion: ProtocolVersion = fiji.getSystemTable().getDataVersion()
      val system20: ProtocolVersion = ProtocolVersion.parse("system-2.0")
      if (curDataVersion.compareTo(system20) < 0) {
        fiji.getSystemTable().setDataVersion(system20)
      }
    } finally {
      fiji.release()
    }
  }

  def environment(uri: FijiURI, fijiSystem: AbstractFijiSystem): Environment = {
    new Environment(
      instanceURI=uri,
      printer=System.out,
      fijiSystem=fijiSystem,
      inputSource=new NullInputSource(),
      modules=List(),
      isInteractive=false)
  }
}
