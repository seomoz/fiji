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

package com.moz.fiji.schema.shell.ddl

import org.apache.hadoop.hbase.HBaseConfiguration
import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.FijiInstaller
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.Environment

/** Creates a new Fiji instance and makes it the active instance. */
@ApiAudience.Private
final class CreateInstanceCommand(val env: Environment, val instanceName: String)
    extends DDLCommand {

  override def exec(): Environment = {
    val instances = env.fijiSystem.listInstances()
    if (instances.contains(instanceName)) {
      throw new DDLException("Instance already exists: " + instanceName)
    }

    val conf = HBaseConfiguration.create()
    val uri = FijiURI.newBuilder().withInstanceName(instanceName).build()
    echo("Creating Fiji instance: " + instanceName + "...")
    FijiInstaller.get().install(uri, conf)

    return env.withInstance(instanceName)
  }
}
