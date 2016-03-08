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

package com.moz.fiji.schema.shell

import scala.collection.JavaConversions._
import org.specs2.mutable._
import org.apache.avro.Schema
import com.moz.fiji.schema.avro.CellSchema
import com.moz.fiji.schema.avro.ColumnDesc
import com.moz.fiji.schema.avro.FamilyDesc
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.layout.InvalidLayoutSchemaException
import com.moz.fiji.schema.shell.api.Client
import com.moz.fiji.schema.shell.avro.XYRecord
import com.moz.fiji.schema.avro.AvroSchema
import com.moz.fiji.schema.shell.util.FijiIntegrationTestHelpers

/**
 * Tests that DDL commands affecting column schemas respect validation requirements and
 * operate correctly on validationg layouts (&gt;= layout-1.3).
 */
class TestSchemaValidation
    extends SpecificationWithJUnit
    with FijiIntegrationTestHelpers {
  "With schema validation enabled, clients" should {

    "create a table correctly" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  MAXVERSIONS = INFINITY,
          |  TTL = FOREVER,
          |  INMEMORY = false,
          |  COMPRESSED WITH GZIP,
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    name "string" WITH DESCRIPTION 'The user\'s name',
          |    email "string",
          |    age "int"),
          |  MAP TYPE FAMILY integers COUNTER
          |);""".stripMargin)

      // Programmatically test proper table creation.
      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName() == "info" }).get

      infoFamily.getColumns().size mustEqual 3

      val nameCol: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "name" }).get

      val cellSchema: CellSchema = nameCol.getColumnSchema()

      // Readers, Writers, Written lists and default schema should all match "string"
      cellSchema.getReaders().size mustEqual 1
      cellSchema.getWriters().size mustEqual 1
      cellSchema.getWritten().size mustEqual 1

      val readerSchema: AvroSchema = cellSchema.getDefaultReader()
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWriters().head mustEqual readerSchema
      cellSchema.getWritten().head mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.STRING))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "refuse an incompatible writer schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      try {
        client.executeUpdate("""
            |CREATE TABLE foo WITH DESCRIPTION 'some data'
            |ROW KEY FORMAT HASHED
            |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
            |  FAMILY info WITH DESCRIPTION 'basic information' (
            |    bar "long")
            |);""".stripMargin)

        // Try to add an obviously-incompatible schema.
        (client.executeUpdate("ALTER TABLE foo ADD WRITER SCHEMA \"string\" FOR COLUMN info:bar")
            must throwA[InvalidLayoutSchemaException])
      } finally {
        client.close()
        fijiSystem.shutdown()
      }

      ok("Test completed")
    }

    "refuse an incompatible reader schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      try {
        client.executeUpdate("""
            |CREATE TABLE foo WITH DESCRIPTION 'some data'
            |ROW KEY FORMAT HASHED
            |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
            |  FAMILY info WITH DESCRIPTION 'basic information' (
            |    bar "long")
            |);""".stripMargin)

        // data written as 'long' cannot necessarily be read as 'int'.
        (client.executeUpdate("ALTER TABLE foo ADD READER SCHEMA \"int\" FOR COLUMN info:bar")
            must throwA[InvalidLayoutSchemaException])
      } finally {
        client.close()
        fijiSystem.shutdown()
      }

      ok("Test completed")
    }

    "refuse a subtly incompatible writer schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      try {
        client.executeUpdate("""
            |CREATE TABLE foo WITH DESCRIPTION 'some data'
            |ROW KEY FORMAT HASHED
            |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
            |  FAMILY info WITH DESCRIPTION 'basic information' (
            |    bar "int")
            |);""".stripMargin)

        // Since "int" is a reader schema, can't add "long" as a writer schema directly.
        // Note that a very similar statement worked ok in the "add a reader schema" test
        (client.executeUpdate("ALTER TABLE foo ADD WRITER SCHEMA \"long\" FOR COLUMN info:bar")
            must throwA[InvalidLayoutSchemaException])
      } finally {
        client.close()
        fijiSystem.shutdown()
      }

      ok("Test completed")
    }

    "add a reader schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "int")
          |);""".stripMargin)

      client.executeUpdate("ALTER TABLE foo ADD READER SCHEMA \"long\" FOR COLUMN info:bar")

      // Test that both reader schemas are present.
      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 2 // Both "int" and "long"
      cellSchema.getWriters().size mustEqual 1 // Just "int"
      cellSchema.getWritten().size mustEqual 1

      val readerSchema: AvroSchema = cellSchema.getDefaultReader() // Should be "int"
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWriters().head mustEqual readerSchema
      cellSchema.getWritten().head mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.INT))

      // Check that "long" is the 2nd schema in the readers list.
      val longSchema: AvroSchema = cellSchema.getReaders()(1)
      (env.fijiSystem.getSchemaFor(env.instanceURI, longSchema).get mustEqual
          Schema.create(Schema.Type.LONG))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "add a writer schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "long")
          |);""".stripMargin)

      client.executeUpdate("ALTER TABLE foo ADD WRITER SCHEMA \"int\" FOR COLUMN info:bar")

      // Test that both writer schemas are present.
      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1 // Just "long"
      cellSchema.getWriters().size mustEqual 2 // Both "int" and "long".
      cellSchema.getWritten().size mustEqual 2

      val readerSchema: AvroSchema = cellSchema.getDefaultReader() // Should be "long"
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWriters().head mustEqual readerSchema
      cellSchema.getWritten().head mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.LONG))

      // Check that "int" is the 2nd schema in the writers list.
      val intSchema: AvroSchema = cellSchema.getWriters()(1)
      (env.fijiSystem.getSchemaFor(env.instanceURI, intSchema).get mustEqual
          Schema.create(Schema.Type.INT))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "drop a writer schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "long")
          |);""".stripMargin)

      client.executeUpdate("ALTER TABLE foo DROP WRITER SCHEMA \"long\" FOR COLUMN info:bar")

      // Test that no writer schemas are present... but it's still in the "written" list.
      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1 // Just "long"
      cellSchema.getWriters().size mustEqual 0 // nada.
      cellSchema.getWritten().size mustEqual 1 // "long" remains in the written list.

      val readerSchema: AvroSchema = cellSchema.getDefaultReader() // Should be "long"
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWritten().head mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.LONG))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "allow a series of changes to reader schemas permitting int-to-long conversion" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "int")
          |);""".stripMargin)

      // We can't add "long" as a writer schema directly, since its reader schema is int.
      // Change that to long, then add it as the approved writer schema
      client.executeUpdate("ALTER TABLE foo ADD DEFAULT READER SCHEMA \"long\" FOR COLUMN info:bar")
      client.executeUpdate("ALTER TABLE foo DROP READER SCHEMA \"int\" FOR COLUMN info:bar")
      client.executeUpdate("ALTER TABLE foo ADD WRITER SCHEMA \"long\" FOR COLUMN info:bar")

      // Verify our result state.
      // * default reader schema should be "long"
      // * the only schema in readers should be "long"
      // * the writers and written lists should be { "int", "long" }

      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1 // Just "long"
      cellSchema.getWriters().size mustEqual 2 // Both "int" and "long".
      cellSchema.getWritten().size mustEqual 2

      val readerSchema: AvroSchema = cellSchema.getDefaultReader() // Should be "long"
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWriters()(1) mustEqual readerSchema
      cellSchema.getWritten()(1) mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.LONG))

      // Check that "int" is the 1st schema in the writers list.
      val intSchema: AvroSchema = cellSchema.getWriters()(0)
      (env.fijiSystem.getSchemaFor(env.instanceURI, intSchema).get mustEqual
          Schema.create(Schema.Type.INT))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "remove the default reader if it is also dropped as a reader schema" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "int")
          |);""".stripMargin)

      // This should also remove it as a default reader schema.
      client.executeUpdate("ALTER TABLE foo DROP READER SCHEMA \"int\" FOR COLUMN info:bar")

      // Verify our result state.
      // * default reader schema should be null
      // * the readers list should be empty.
      // * the writers and written lists should be { "int" }

      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 0
      cellSchema.getWriters().size mustEqual 1 // "int"
      cellSchema.getWritten().size mustEqual 1

      cellSchema.getDefaultReader() must beNull

      val writerSchema: AvroSchema = cellSchema.getWriters()(0)
      cellSchema.getWritten()(0) mustEqual writerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, writerSchema).get mustEqual
          Schema.create(Schema.Type.INT))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "allow creation of empty schema lists for a column" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar )
          |);""".stripMargin)

      // Verify intermediate state: no reader, writer, written schemas for info:bar.

      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 0
      cellSchema.getWriters().size mustEqual 0
      cellSchema.getWritten().size mustEqual 0

      cellSchema.getDefaultReader() must beNull

      // We should be able to add any schema we want.
      // Add to both the readers and writers lists.
      client.executeUpdate("ALTER TABLE foo ADD SCHEMA \"int\" FOR COLUMN info:bar")

      // Verify our result state.
      // readers, writers, written should all be "int". default_reader should still be null.
      val layout2: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily2: FamilyDesc = layout2.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col2: ColumnDesc = infoFamily2.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema2: CellSchema = col2.getColumnSchema()

      cellSchema2.getReaders().size mustEqual 1
      cellSchema2.getWriters().size mustEqual 1
      cellSchema2.getWritten().size mustEqual 1

      // ADD SCHEMA will set the reader and writer, but not a default reader schema.
      cellSchema2.getDefaultReader() must beNull

      val reader: AvroSchema = cellSchema2.getReaders()(0)
      (env.fijiSystem.getSchemaFor(env.instanceURI, reader).get mustEqual
          Schema.create(Schema.Type.INT))

      reader mustEqual cellSchema2.getWriters()(0)
      reader mustEqual cellSchema2.getWritten()(0)

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "allow setting schema by class" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar )
          |);""".stripMargin)

      client.executeUpdate("""
          |ALTER TABLE foo ADD SCHEMA CLASS com.moz.fiji.schema.shell.avro.XYRecord
          |FOR COLUMN info:bar
          |""".stripMargin)
      client.executeUpdate("""
          |ALTER TABLE foo ADD DEFAULT READER SCHEMA CLASS com.moz.fiji.schema.shell.avro.XYRecord
          |FOR COLUMN info:bar
          |""".stripMargin)

      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1
      cellSchema.getWriters().size mustEqual 1
      cellSchema.getWritten().size mustEqual 1

      val reader: AvroSchema = cellSchema.getDefaultReader()

      cellSchema.getReaders().head mustEqual reader
      cellSchema.getWriters().head mustEqual reader
      cellSchema.getWritten().head mustEqual reader

      val readerSchemaName: String = cellSchema.getSpecificReaderSchemaClass().toString()
      readerSchemaName mustEqual classOf[XYRecord].getName()

      (env.fijiSystem.getSchemaFor(env.instanceURI, reader).get mustEqual
          XYRecord.SCHEMA$)

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "allow Avro schema via DSL" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (bar)
          |);""".stripMargin)

      client.executeUpdate("""
          |ALTER TABLE foo ADD SCHEMA AVRO array<int> FOR COLUMN info:bar
          |""".stripMargin)
      client.executeUpdate("""
          |ALTER TABLE foo ADD DEFAULT READER SCHEMA AVRO array<int> FOR COLUMN info:bar
          |""".stripMargin)

      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1
      cellSchema.getWriters().size mustEqual 1
      cellSchema.getWritten().size mustEqual 1

      val reader: AvroSchema = cellSchema.getDefaultReader()

      cellSchema.getReaders().head mustEqual reader
      cellSchema.getWriters().head mustEqual reader
      cellSchema.getWritten().head mustEqual reader

      cellSchema.getSpecificReaderSchemaClass mustEqual null

      (env.fijiSystem.getSchemaFor(env.instanceURI, reader).get mustEqual
          Schema.createArray(Schema.create(Schema.Type.INT)))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "let the user add a schema redundantly" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "int")
          |);""".stripMargin)

      // If this schema is already present, it should be benign.
      client.executeUpdate("ALTER TABLE foo ADD SCHEMA \"int\" FOR COLUMN info:bar")

      // Test that the schema list is just "int" for readers and writers.
      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1 // Just "int"
      cellSchema.getWriters().size mustEqual 1
      cellSchema.getWritten().size mustEqual 1

      val readerSchema: AvroSchema = cellSchema.getDefaultReader() // Should be "int"
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWriters().head mustEqual readerSchema
      cellSchema.getWritten().head mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.INT))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }

    "let the user drop a schema that wasn't attached" in {
      val uri = getNewInstanceURI()
      val fijiSystem = getFijiSystem()
      val client = Client.newInstanceWithSystem(uri, fijiSystem)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    bar "int")
          |);""".stripMargin)

      // If this schema is not present (e.g, redundant drop) it should be benign.
      client.executeUpdate("ALTER TABLE foo DROP SCHEMA \"string\" FOR COLUMN info:bar")

      // Test that the schema list is just "int" for readers and writers.
      val env = environment(uri, fijiSystem)
      val layout: TableLayoutDesc = env.fijiSystem.getTableLayout(uri, "foo").get.getDesc
      val infoFamily: FamilyDesc = layout.getLocalityGroups().head.getFamilies().find({ grp =>
          grp.getName().toString() == "info" }).get
      val col: ColumnDesc = infoFamily.getColumns().find({ col =>
          col.getName().toString() == "bar" }).get

      val cellSchema: CellSchema = col.getColumnSchema()

      cellSchema.getReaders().size mustEqual 1 // Just "int"
      cellSchema.getWriters().size mustEqual 1
      cellSchema.getWritten().size mustEqual 1

      val readerSchema: AvroSchema = cellSchema.getDefaultReader() // Should be "int"
      cellSchema.getReaders().head mustEqual readerSchema
      cellSchema.getWriters().head mustEqual readerSchema
      cellSchema.getWritten().head mustEqual readerSchema

      (env.fijiSystem.getSchemaFor(env.instanceURI, readerSchema).get mustEqual
          Schema.create(Schema.Type.INT))

      client.close()
      env.fijiSystem.shutdown()

      ok("Test completed")
    }
  }
}
