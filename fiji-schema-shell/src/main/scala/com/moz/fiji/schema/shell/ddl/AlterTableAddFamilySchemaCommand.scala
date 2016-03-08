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
import com.moz.fiji.schema.avro.CellSchema
import com.moz.fiji.schema.avro.FamilyDesc
import com.moz.fiji.schema.avro.TableLayoutDesc

import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.SchemaUsageFlags

/**
 * Add a reader or writer schema to the list of approved schemas for a column.
 */
@ApiAudience.Private
final class AlterTableAddFamilySchemaCommand(
    val env: Environment,
    val tableName: String,
    val schemaFlags: SchemaUsageFlags,
    val familyName: String,
    val schema: SchemaSpec) extends TableDDLCommand {

  override def validateArguments(): Unit = {
    val layout = getInitialLayout()
    val cellSchemaContext: CellSchemaContext = CellSchemaContext.create(env, layout, schemaFlags)

    checkColFamilyIsMapType(layout, familyName)
    if (!cellSchemaContext.supportsLayoutValidation()) {
      throw new DDLException("Cannot run ALTER TABLE.. ADD SCHEMA on a table layout "
          + "that does not support schema validation.")
    }
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    val cellSchemaContext: CellSchemaContext = CellSchemaContext.create(env, layout, schemaFlags)
    val curFamilyDesc: FamilyDesc = getFamily(layout, familyName)
        .getOrElse(throw new DDLException("No such family: " + familyName))
    val curMapSchema: CellSchema = curFamilyDesc.getMapSchema()
    schema.addToCellSchema(curMapSchema, cellSchemaContext)
  }
}
