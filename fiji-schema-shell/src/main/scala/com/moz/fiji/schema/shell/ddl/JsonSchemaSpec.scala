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

import java.util.{List => JList}

import org.apache.avro.Schema

import com.google.common.collect.Lists

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.avro.AvroSchema
import com.moz.fiji.schema.avro.AvroValidationPolicy
import com.moz.fiji.schema.avro.CellSchema
import com.moz.fiji.schema.avro.SchemaStorage
import com.moz.fiji.schema.avro.SchemaType

import com.moz.fiji.schema.shell.DDLException

/** A schema specified as its Avro json representation. */
@ApiAudience.Private
final class JsonSchemaSpec(val json: String) extends SchemaSpec {
  override def toString(): String = { json }

  override def toNewCellSchema(cellSchemaContext: CellSchemaContext): CellSchema = {
    if (cellSchemaContext.supportsLayoutValidation()) {
      // layout-1.3 and up: use validation-enabled specification.
      val avroValidationPolicy: AvroValidationPolicy =
          cellSchemaContext.getValidationPolicy().avroValidationPolicy

      val schema: Schema = new Schema.Parser().parse(json)

      // Use the schema table to find the actual uid associated with this schema.
      val uidForSchema: Long = cellSchemaContext.env.fijiSystem.getOrCreateSchemaId(
          cellSchemaContext.env.instanceURI, schema)

      val avroSchema = AvroSchema.newBuilder().setUid(uidForSchema).build()

      // Register the specified class as a valid reader and writer schema as well as the
      // default reader schema.
      val readers: JList[AvroSchema] = Lists.newArrayList(avroSchema)
      val writers: JList[AvroSchema] = Lists.newArrayList(avroSchema)

      // For now (layout-1.3), adding to the writers list => adding to the "written" list.
      val written: JList[AvroSchema] = Lists.newArrayList(avroSchema)

      return CellSchema.newBuilder()
          .setStorage(SchemaStorage.UID)
          .setType(SchemaType.AVRO)
          .setValue(null)
          .setAvroValidationPolicy(avroValidationPolicy)
          .setDefaultReader(avroSchema)
          .setReaders(readers)
          .setWritten(written)
          .setWriters(writers)
          .build()
    } else {
      // layout-1.2 and prior; use older specification.
      return CellSchema.newBuilder
          .setType(SchemaType.INLINE)
          .setStorage(SchemaStorage.UID)
          .setValue(json)
          .build()
    }
  }

  override def addToCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    if (!cellSchemaContext.supportsLayoutValidation()) {
      // This method was called in an inappropriate context; the CellSchema
      // should be overwritten entirely in older layouts.
      throw new DDLException("SchemaSpec.addToCellSchema() can only be used on validating layouts")
    }

    val avroSchema: Schema = new Schema.Parser().parse(json)

    // Add to the main reader/writer/etc lists.
    addAvroToCellSchema(avroSchema, cellSchema, cellSchemaContext.schemaUsageFlags,
        cellSchemaContext.env)

    return cellSchema
  }

  override def dropFromCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {

    if (!cellSchemaContext.supportsLayoutValidation()) {
      // This method was called in an inappropriate context; the CellSchema
      // should be overwritten entirely in older layouts.
      throw new DDLException("SchemaSpec.dropFromCellSchema() can only be used "
          + "on validating layouts")
    }

    val avroSchema: Schema = new Schema.Parser().parse(json)

    // Use the schema table to find the actual uid associated with this schema.
    // If this returns "None" then the schema never existed -- do nothing.
    val maybeUidForSchemaClass: Option[Long] =
        cellSchemaContext.env.fijiSystem.getSchemaId(cellSchemaContext.env.instanceURI, avroSchema)
    if (maybeUidForSchemaClass.isEmpty) {
      // Nothing to do; this schema is not registered in the schema table.
      return cellSchema
    }

    val uidForSchemaClass: Long = maybeUidForSchemaClass.get

    // Drop from the main reader/writer/etc lists.
    dropAvroFromCellSchema(avroSchema, cellSchema, cellSchemaContext.schemaUsageFlags,
        cellSchemaContext.env)

    return cellSchema
  }

}
