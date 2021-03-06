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

import scala.collection.JavaConversions._

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.layout.FijiTableLayout

import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.Environment

@ApiAudience.Private
final class AlterTableDescForLocalityGroupCommand(
    val env: Environment,
    val tableName: String,
    val localityGroupName: String,
    val description: String) extends TableDDLCommand {

  override def validateArguments(): Unit = {
    val layout = getInitialLayout()
    checkLocalityGroupExists(layout, localityGroupName)
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    getLocalityGroup(layout, localityGroupName).getOrElse(throw new DDLException("Missing group"))
        .setDescription(description)
  }
}
