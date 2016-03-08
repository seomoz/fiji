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

package com.moz.fiji.express.flow

import java.util.UUID

import scala.collection.JavaConverters._

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.InputSourceValidationJob
import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.avro.SimpleRecord
import com.moz.fiji.express.avro.SimpleRecordEvolved1
import com.moz.fiji.express.avro.SimpleRecordEvolved2
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class FijiInputSuite
    extends FijiClientTest
    with FijiSuite {
  import FijiInputSuite._

  def fijiInputTest[T](
      testName: String,
      sourceConstructor: FijiURI => FijiSource,
      expectedFields: Fields,
      expectedValues: Set[T],
      testHdfsMode: Boolean = true,
      testLocalMode: Boolean = true
  )(implicit
      conv: TupleConverter[T],
      set: TupleSetter[Unit]
  ) {
    if (testHdfsMode) {
      test("[HDFS] " + testName) {
        val nilArgsWithMode =
            Mode.putMode(Hdfs(strict = true, conf = new JobConf(getConf)), Args(Nil))
        // Set the global mode variable in case other methods rely on it being injected via an
        // implicit evidence parameter.
        val uri = setupTestTable(getFiji)

        val source = sourceConstructor(uri)
        new InputSourceValidationJob[T](
            source,
            expectedValues,
            expectedFields,
            nilArgsWithMode
        )(
            conv,
            set
        ).run
      }
    }

    if (testLocalMode) {
      test("[Local] " + testName) {
        val nilArgsWithMode = Mode.putMode(Local(strictSources = true), Args(Nil))
        // Set the global mode variable in case other methods rely on it being injected via an
        // implicit evidence parameter.
        val uri = setupTestTable(getFiji)

        val source = sourceConstructor(uri)
        new InputSourceValidationJob[T](
            source,
            expectedValues,
            expectedFields,
            nilArgsWithMode
        )(
            conv,
            set
        ).run
      }
    }
  }

  // Hook into FijiClientTest since methods marked with JUnit's @Before and @After annotations won't
  // run when using ScalaTest.
  setupFijiTest()

  // Decrease the jobtracker poll time.
  getConf.setInt("mapreduce.client.completion.pollinterval", 250)

  fijiInputTest(
      testName = "FijiSource can read cells with default options",
      sourceConstructor = { tableUri: FijiURI =>
        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId(1, "row2"), slice("info:strings", 4L -> new Utf8("string4")))
      )
  )


  // -- TimeRangeSpec --

  fijiInputTest(
      testName = "FijiSource can filter cells with the 'All' time range",
      sourceConstructor = { tableUri: FijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.All)
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId(1, "row1"),
              slice("info:strings", 2L -> new Utf8("string2"), 1L -> new Utf8("string1"))
          ),
          (
              EntityId(1, "row2"),
              slice("info:strings", 4L -> new Utf8("string4"), 3L -> new Utf8("string3"))
          )
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'At' time range",
      sourceConstructor = { tableUri: FijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.At(2L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row1"), slice("info:strings", 2L -> new Utf8("string2")))
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'Before' time range",
      sourceConstructor = { tableUri: FijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.Before(3L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId(1, "row1"),
              slice("info:strings", 2L -> new Utf8("string2"), 1L -> new Utf8("string1"))
          )
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'Between' time range",
      sourceConstructor = { tableUri: FijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.Between(2L, 4L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId(1, "row2"), slice("info:strings", 3L -> new Utf8("string3")))
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'From' time range",
      sourceConstructor = { tableUri: FijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.From(2L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId(1, "row1"),
              slice("info:strings", 2L -> new Utf8("string2"))
          ),
          (
              EntityId(1, "row2"),
              slice("info:strings", 4L -> new Utf8("string4"), 3L -> new Utf8("string3"))
          )
      )
  )


  // -- RowRangeSpec --

  fijiInputTest(
      testName = "FijiSource can filter cells with the 'All' row range",
      sourceConstructor = { tableUri: FijiURI =>
        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withRowRangeSpec(RowRangeSpec.All)
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId(1, "row2"), slice("info:strings", 4L -> new Utf8("string4")))
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'Before' row range",
      sourceConstructor = { tableUri: FijiURI =>
        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withRowRangeSpec(RowRangeSpec.Before(EntityId(1, "row2")))
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row1"), slice("info:strings", 2L -> new Utf8("string2")))
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'Between' row range",
      sourceConstructor = { tableUri: FijiURI =>
        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withRowRangeSpec(RowRangeSpec.Between(EntityId(1, "row2"), EntityId(1, "row3")))
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row2"), slice("info:strings", 4L -> new Utf8("string4")))
      )
  )
  fijiInputTest(
      testName = "FijiSource can filter cells with the 'From' row range",
      sourceConstructor = { tableUri: FijiURI =>
        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withRowRangeSpec(RowRangeSpec.From(EntityId(1, "row3")))
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])]()
  )


  // -- RowFilterSpec --

  fijiInputTest(
      testName = "FijiSource can filter cells with the 'NoFilter' row filter",
      sourceConstructor = { tableUri: FijiURI =>
        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withRowFilterSpec(RowFilterSpec.NoFilter)
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId(1, "row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId(1, "row2"), slice("info:strings", 4L -> new Utf8("string4")))
      )
  )
  // TODO: Add test for 'Random' row filter.
  // TODO: Add test for 'FijiSchema' row filter.
  // TODO: Add test for 'And' row filter.
  // TODO: Add test for 'Or' row filter.


  // -- QualifiedColumnInputSpec --

  fijiInputTest(
      testName = "FijiSource can read more than one cell from a column by setting max versions > 1",
      sourceConstructor = { tableUri: FijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(2)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId(1, "row1"),
              slice("info:strings", 2L -> new Utf8("string2"), 1L -> new Utf8("string1"))
          ),
          (
              EntityId(1, "row2"),
              slice("info:strings", 4L -> new Utf8("string4"), 3L -> new Utf8("string3"))
          )
      )
  )
  // TODO: Add test for paging.
  fijiInputTest(
      testName = "FijiSource can read records from a column using the default column reader schema",
      sourceConstructor = { tableUri: FijiURI =>
        val recordsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "records")
            .withSchemaSpec(SchemaSpec.DefaultReader)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(recordsColumn -> 'records)
            .build
      },
      expectedFields = new Fields("entityId", "records"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (EntityId(1, "row1"), slice("info:records", 2L -> testRecord2GE2)),
          (EntityId(1, "row2"), slice("info:records", 4L -> testRecord4GE2))
      )
  )
  fijiInputTest(
      testName = "FijiSource can read records from a column using the column's cell writer schema",
      sourceConstructor = { tableUri: FijiURI =>
        val recordsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "records")
            .withSchemaSpec(SchemaSpec.Writer)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(recordsColumn -> 'records)
            .build
      },
      expectedFields = new Fields("entityId", "records"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (EntityId(1, "row1"), slice("info:records", 2L -> testRecord2G)),
          (EntityId(1, "row2"), slice("info:records", 4L -> testRecord4G))
      )
  )
  fijiInputTest(
      testName = "FijiSource can read records from a column using a provided GenericRecord schema",
      sourceConstructor = { tableUri: FijiURI =>
        val recordsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "records")
            .withSchemaSpec(SchemaSpec.Generic(SimpleRecordEvolved1.getClassSchema))
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(recordsColumn -> 'records)
            .build
      },
      expectedFields = new Fields("entityId", "records"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (EntityId(1, "row1"), slice("info:records", 2L -> testRecord2GE1)),
          (EntityId(1, "row2"), slice("info:records", 4L -> testRecord4GE1))
      )
  )
  fijiInputTest(
      testName = "FijiSource can read records from a column using a provided SpecificRecord schema",
      sourceConstructor = { tableUri: FijiURI =>
        val recordsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "records")
            .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecordEvolved1]))
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(recordsColumn -> 'records)
            .build
      },
      expectedFields = new Fields("entityId", "records"),
      expectedValues = Set[(EntityId, Seq[FlowCell[SimpleRecordEvolved1]])](
          (EntityId(1, "row1"), slice("info:records", 2L -> testRecord2SE1)),
          (EntityId(1, "row2"), slice("info:records", 4L -> testRecord4SE1))
      )
  )
  // Filters don't make sense for qualified columns right now.


  // -- ColumnFamilyInputSpec --

  fijiInputTest(
      testName = "FijiSource can read more than one cell from a family by setting max versions > 1",
      sourceConstructor = { tableUri: FijiURI =>
        val mapFamily = ColumnFamilyInputSpec.builder
            .withFamily("mapfamily")
            .withMaxVersions(2)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(mapFamily -> 'mapfamily)
            .build
      },
      expectedFields = new Fields("entityId", "mapfamily"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (
              EntityId(1, "row1"),
              mapSlice("mapfamily",
                  ("qual1", 2L, testRecord2G),
                  ("qual1", 1L, testRecord1G),
                  ("qual2", 4L, testRecord4G),
                  ("qual2", 3L, testRecord3G)
              )
          ),
          (
              EntityId(1, "row2"),
              mapSlice("mapfamily",
                  ("qual1", 2L, testRecord2G),
                  ("qual1", 1L, testRecord1G),
                  ("qual2", 4L, testRecord4G),
                  ("qual2", 3L, testRecord3G)
              )
          )
      )
  )
  // TODO: Add test for paging.
  fijiInputTest(
      testName = "FijiSource can read records from a family using the default family reader schema",
      sourceConstructor = { tableUri: FijiURI =>
        val mapFamily = ColumnFamilyInputSpec.builder
            .withFamily("mapfamily")
            .withSchemaSpec(SchemaSpec.DefaultReader)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(mapFamily -> 'mapfamily)
            .build
      },
      expectedFields = new Fields("entityId", "mapfamily"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (
              EntityId(1, "row1"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2GE2), ("qual2", 4L, testRecord4GE2))
          ),
          (
              EntityId(1, "row2"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2GE2), ("qual2", 4L, testRecord4GE2))
          )
      )
  )
  fijiInputTest(
      testName = "FijiSource can read records from a family using the family's cell writer schema",
      sourceConstructor = { tableUri: FijiURI =>
        val mapFamily = ColumnFamilyInputSpec.builder
            .withFamily("mapfamily")
            .withSchemaSpec(SchemaSpec.Writer)
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(mapFamily -> 'mapfamily)
            .build
      },
      expectedFields = new Fields("entityId", "mapfamily"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (
              EntityId(1, "row1"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2G), ("qual2", 4L, testRecord4G))
          ),
          (
              EntityId(1, "row2"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2G), ("qual2", 4L, testRecord4G))
          )
      )
  )
  fijiInputTest(
      testName = "FijiSource can read records from a family using a provided GenericRecord schema",
      sourceConstructor = { tableUri: FijiURI =>
        val mapFamily = ColumnFamilyInputSpec.builder
            .withFamily("mapfamily")
            .withSchemaSpec(SchemaSpec.Generic(SimpleRecordEvolved1.getClassSchema))
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(mapFamily -> 'mapfamily)
            .build
      },
      expectedFields = new Fields("entityId", "mapfamily"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (
              EntityId(1, "row1"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2GE1), ("qual2", 4L, testRecord4GE1))
          ),
          (
              EntityId(1, "row2"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2GE1), ("qual2", 4L, testRecord4GE1))
          )
      )
  )
  fijiInputTest(
      testName = "FijiSource can read records from a family using a provided SpecificRecord schema",
      sourceConstructor = { tableUri: FijiURI =>
        val mapFamily = ColumnFamilyInputSpec.builder
            .withFamily("mapfamily")
            .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecordEvolved1]))
            .build

        FijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumnSpecs(mapFamily -> 'mapfamily)
            .build
      },
      expectedFields = new Fields("entityId", "mapfamily"),
      expectedValues = Set[(EntityId, Seq[FlowCell[GenericRecord]])](
          (
              EntityId(1, "row1"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2SE1), ("qual2", 4L, testRecord4SE1))
          ),
          (
              EntityId(1, "row2"),
              mapSlice("mapfamily", ("qual1", 2L, testRecord2SE1), ("qual2", 4L, testRecord4SE1))
          )
      )
  )
  // TODO: Add tests for column filters

  // TODO: Add tests for error cases (invalid table uri, invalid column name, etc).
}

object FijiInputSuite {
  def toGeneric(record: SpecificRecord): GenericRecord = {
    val schema = record.getSchema
    schema
        .getFields
        .asScala
        .foldLeft(new GenericRecordBuilder(schema)) { (builder, field) =>
          builder.set(field, record.get(field.pos()))
        }
        .build()
  }

  val testRecord1S = SimpleRecord.newBuilder().setL(1L).setS("1").build()
  val testRecord1SE1 = SimpleRecordEvolved1.newBuilder().setL(1L).setS("1").build()
  val testRecord1SE2 = SimpleRecordEvolved2.newBuilder().setL(1L).setS("1").build()
  val testRecord1G = toGeneric(testRecord1S)
  val testRecord1GE1 = toGeneric(testRecord1SE1)
  val testRecord1GE2 = toGeneric(testRecord1SE2)
  val testRecord2S = SimpleRecord.newBuilder().setL(2L).setS("2").build()
  val testRecord2SE1 = SimpleRecordEvolved1.newBuilder().setL(2L).setS("2").build()
  val testRecord2SE2 = SimpleRecordEvolved2.newBuilder().setL(2L).setS("2").build()
  val testRecord2G = toGeneric(testRecord2S)
  val testRecord2GE1 = toGeneric(testRecord2SE1)
  val testRecord2GE2 = toGeneric(testRecord2SE2)
  val testRecord3S = SimpleRecord.newBuilder().setL(3L).setS("3").build()
  val testRecord3SE1 = SimpleRecordEvolved1.newBuilder().setL(3L).setS("3").build()
  val testRecord3SE2 = SimpleRecordEvolved2.newBuilder().setL(3L).setS("3").build()
  val testRecord3G = toGeneric(testRecord3S)
  val testRecord3GE1 = toGeneric(testRecord3SE1)
  val testRecord3GE2 = toGeneric(testRecord3SE2)
  val testRecord4S = SimpleRecord.newBuilder().setL(4L).setS("4").build()
  val testRecord4SE1 = SimpleRecordEvolved1.newBuilder().setL(4L).setS("4").build()
  val testRecord4SE2 = SimpleRecordEvolved2.newBuilder().setL(4L).setS("4").build()
  val testRecord4G = toGeneric(testRecord4S)
  val testRecord4GE1 = toGeneric(testRecord4SE1)
  val testRecord4GE2 = toGeneric(testRecord4SE2)

  def setupTestTable(fiji: Fiji): FijiURI = {
    val testTableName = "%s_%s"
        .format(
            this.getClass.getSimpleName,
            UUID.randomUUID().toString
        )
        .replace("-", "_")
        .replace("$", "_")
    val testTableDDL =
      """
        |CREATE TABLE %s
        |    ROW KEY FORMAT (
        |        dummy INT,
        |        row_name STRING,
        |        HASH (THROUGH dummy, SIZE = 1)
        |    )
        |    PROPERTIES (
        |        VALIDATION = STRICT
        |    )
        |    WITH LOCALITY GROUP default (
        |        MAXVERSIONS = INFINITY,
        |        TTL = FOREVER,
        |        COMPRESSED WITH NONE,
        |        FAMILY info
        |        (
        |            strings "string",
        |            numbers "int",
        |            records WITH SCHEMA CLASS com.moz.fiji.express.avro.SimpleRecord
        |        ),
        |        MAP TYPE FAMILY mapfamily WITH SCHEMA CLASS com.moz.fiji.express.avro.SimpleRecord
        |    );
        |ALTER TABLE %s
        |    ADD DEFAULT READER SCHEMA CLASS com.moz.fiji.express.avro.SimpleRecordEvolved2
        |    FOR COLUMN info:records;
        |ALTER TABLE %s
        |    ADD DEFAULT READER SCHEMA CLASS com.moz.fiji.express.avro.SimpleRecordEvolved2
        |    FOR FAMILY mapfamily;
      """.stripMargin.format(testTableName, testTableName, testTableName)

    // Create table.
    ResourceUtil.executeDDLString(fiji, testTableDDL)

    // Populate table.
    ResourceUtil.withFijiTable(fiji, testTableName) { table =>
      new InstanceBuilder(fiji)
          .withTable(table)
              // Use a dummy hashed field so that sorting on the actual string id makes sense.
              .withRow(1: java.lang.Integer, "row1")
                  .withFamily("info")
                      .withQualifier("strings")
                          .withValue(1L, "string1")
                          .withValue(2L, "string2")
                      .withQualifier("numbers")
                          .withValue(1L, 1)
                          .withValue(2L, 2)
                      .withQualifier("records")
                          .withValue(1L, testRecord1S)
                          .withValue(2L, testRecord2S)
                  .withFamily("mapfamily")
                      .withQualifier("qual1")
                          .withValue(1L, testRecord1S)
                          .withValue(2L, testRecord2S)
                      .withQualifier("qual2")
                          .withValue(3L, testRecord3S)
                          .withValue(4L, testRecord4S)
              .withRow(1: java.lang.Integer, "row2")
                  .withFamily("info")
                      .withQualifier("strings")
                          .withValue(3L, "string3")
                          .withValue(4L, "string4")
                      .withQualifier("numbers")
                          .withValue(3L, 3)
                          .withValue(4L, 4)
                      .withQualifier("records")
                          .withValue(3L, testRecord3S)
                          .withValue(4L, testRecord4S)
                  .withFamily("mapfamily")
                      .withQualifier("qual1")
                          .withValue(1L, testRecord1S)
                          .withValue(2L, testRecord2S)
                      .withQualifier("qual2")
                          .withValue(3L, testRecord3S)
                          .withValue(4L, testRecord4S)
          .build()
    }

    FijiURI
        .newBuilder(fiji.getURI)
        .withTableName(testTableName)
        .build()
  }
}
