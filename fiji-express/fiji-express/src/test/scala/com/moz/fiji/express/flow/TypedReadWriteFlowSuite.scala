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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.moz.fiji.express.flow

import java.util.UUID

import scala.Some
import scala.collection.JavaConversions.asScalaIterator

import org.junit
import org.junit.Before
import com.twitter.scalding.TypedSink
import com.twitter.scalding.TypedPipe
import com.twitter.scalding.Mode
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import org.apache.avro.util.Utf8
import org.apache.hadoop.mapred.JobConf

import com.moz.fiji.schema.{EntityId => JEntityId}
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiDataRequestBuilder
import com.moz.fiji.schema.FijiRowData
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.express.avro.SimpleRecord
import com.moz.fiji.schema.util.InstanceBuilder
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef
import com.moz.fiji.express.flow.util.ResourceUtil

class TypedReadWriteFlowSuite extends FijiClientTest{

  private var fiji: Fiji = null

  private def fijiSink (fijiUri:FijiURI): TypedSink[Seq[ExpressColumnOutput[_]]] = {
    FijiOutput.typedSinkForTable(fijiUri)
  }

  @Before
  def setupFijiForTypedTest(): Unit = {
    fiji = createTestFiji()
  }

  def setupFijiTableForTest(): FijiTable = {
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
          |    ROW KEY FORMAT RAW
          |    PROPERTIES (
          |        VALIDATION = STRICT
          |    )
          |    WITH LOCALITY GROUP default (
          |        MAXVERSIONS = INFINITY,
          |        TTL = FOREVER,
          |        COMPRESSED WITH NONE,
          |        FAMILY family
          |        (
          |            column column1 "string",
          |            column column2 "string",
          |            column column3 WITH SCHEMA CLASS com.moz.fiji.express.avro.SimpleRecord,
          |            column column4 WITH SCHEMA CLASS com.moz.fiji.express.avro.SimpleRecord
          |        )
          |    );
        """.stripMargin.format(testTableName, testTableName, testTableName)

    ResourceUtil.executeDDLString(fiji, testTableDDL)
    fiji.openTable(testTableName)
  }

  /**
   * Default input source used for the tests in this suite.
   *
   * @param fijiUri The uri for the fiji table.
   */
  private def defaultTestColumnInputSource(fijiUri: FijiURI) = FijiInput.typedBuilder
      .withTableURI(fijiUri)
      .withColumnSpecs(
          List(
              QualifiedColumnInputSpec
                  .builder
                  .withColumn("family", "column1")
                  .withMaxVersions(Int.MaxValue)
                  .build,
              QualifiedColumnInputSpec
                  .builder
                  .withColumn("family", "column2")
                  .withMaxVersions(Int.MaxValue)
                  .build,
              QualifiedColumnInputSpec
                  .builder
                  .withColumn("family", "column3")
                  .withMaxVersions(Int.MaxValue)
                  .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
                  .build)
      ).build


  def validateAndDeleteWrites[T](
      row: String,
      columns: Seq[FijiColumnName],
      table: FijiTable,
      expected: Set[_]
  ): Unit = {
    ResourceUtil.doAndClose(table.openTableReader()) { reader =>
      val dataRequestBuilder: FijiDataRequestBuilder = FijiDataRequest.builder()
      columns.foreach { col: FijiColumnName =>
        dataRequestBuilder
            .addColumns(ColumnsDef.create()
                .withMaxVersions(Int.MaxValue)
                .add(col))}

      val rowData: FijiRowData = reader.get(
          table.getEntityId(row),
          dataRequestBuilder.build())

      val fijiCellSet: Set[FijiCell[_]] = columns.flatMap{ colName: FijiColumnName =>
        rowData.iterator[T](colName.getFamily, colName.getQualifier).toList
      }.toSet

      val actualSet:Set[_] = fijiCellSet.map { cell : FijiCell[_] => cell.getData}
      val entityId: JEntityId = table.getEntityId(row)

      ResourceUtil.doAndClose(table.openTableWriter()) { writer =>
        fijiCellSet.foreach { cell: FijiCell[_] =>
          writer.deleteCell(
              entityId,
              cell.getColumn.getFamily,
              cell.getColumn.getQualifier,
              cell.getTimestamp)
        }
      }
      assert(
          actualSet == expected,
          "actual: %s\nexpected: %s\noutput.".format(
              actualSet,
              expected))
    }
  }

  /**
   * Runs the test using the functions passed in as the parameters and validates the output.
   *
   * @param fijiTable The Fiji table used for the test.
   * @param jobInput  A function that takes in FijiUri as a parameter and returns an instance of
   *     TypedFijiSource.
   * @param jobBody A function that maps the input of the job to a Seq[ExpressColumnOutput] for the
   *     purpose of writing it to the sink.
   * @param sink The sink to be used for writing the test data.
   */
  def runTypedTest[T](
      fijiTable: FijiTable,
      jobInput: FijiURI => TypedFijiSource[ExpressResult],
      jobBody: TypedPipe[ExpressResult] => TypedPipe[Seq[ExpressColumnOutput[T]]],
      sink: TypedSink[Seq[ExpressColumnOutput[T]]],
      //validation params
      validationRow: String,
      validationColumns: Seq[FijiColumnName],
      expectedSet: Set[_]
  ): Unit = {
    class TypedFijiTest(args: Args) extends FijiJob(args) {
      jobBody(jobInput(fijiTable.getURI)).write(sink)
    }

    // Run in local mode.
    val localModeArgs = Mode.putMode(Local(strictSources = true), Args(List()))
    new TypedFijiTest(localModeArgs).run
    validateAndDeleteWrites(validationRow, validationColumns, fijiTable, expectedSet)

    //Run in Hdfs mode
    val hdfsModeArgs =
        Mode.putMode(Hdfs(strict = true, conf = new JobConf(getConf)), Args(Nil))
    new TypedFijiTest(hdfsModeArgs).run
    validateAndDeleteWrites(validationRow, validationColumns, fijiTable, expectedSet)
  }


  @junit.Test
  def testReadWriteTwoColumns() {

    val fijiTable: FijiTable = setupFijiTableForTest()

    new InstanceBuilder(fiji)
        .withTable(fijiTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1")
                        .withValue("v1")
                    .withQualifier("column2")
                        .withValue("v2")
                    .build()

    def body(source: TypedPipe[ExpressResult]): TypedPipe[Seq[ExpressColumnOutput[Utf8]]] = {
      source.map { row: ExpressResult =>
        val mostRecent1 = row.mostRecentCell[Utf8]("family", "column1")
        val col1 = ExpressColumnOutput(
            EntityId("row2"),
            "family",
            "column1",
            mostRecent1.datum,
            version = Some(mostRecent1.version))

        val mostRecent2 = row.mostRecentCell[Utf8]("family", "column2")
        val col2 = ExpressColumnOutput(
            EntityId("row2"),
            "family",
            "column2",
            mostRecent2.datum,
            version = Some(mostRecent2.version))
        Seq(col1, col2)
      }
    }

    val columns: Seq[FijiColumnName] = List("column1", "column2").map{ col: String =>
      FijiColumnName.create("family", col)}
    try{
      runTypedTest[Utf8](
          fijiTable,
          defaultTestColumnInputSource,
          body,
          fijiSink(fijiTable.getURI),
          validationRow = "row2",
          validationColumns = columns,
          expectedSet = Array("v1", "v2").map { s => new Utf8(s) }.toSet)
    }
    finally {
      fijiTable.release()
    }
  }


  @junit.Test
  def testMultipleWritesOneColumn(): Unit = {
    val fijiTable: FijiTable = setupFijiTableForTest()

    new InstanceBuilder(fiji)
        .withTable(fijiTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1")
                        .withValue(1l, "v1")
                        .withValue(2l, "v2")
                        .withValue(3l, "v3")
                        .withValue(4l, "v4")
                        .build()

    def body(source: TypedPipe[ExpressResult]): TypedPipe[Seq[ExpressColumnOutput[Utf8]]] = {
      source.map { row: ExpressResult =>
        row.qualifiedColumnCells[Utf8]("family", "column1")
            .map {
              cell: FlowCell[Utf8] =>
                ExpressColumnOutput(
                    EntityId("row1"),
                    "family",
                    "column2",
                    cell.datum,
                    version = Some(cell.version))
        }.toSeq
      }
    }

    try{
      runTypedTest[Utf8](
          fijiTable,
          defaultTestColumnInputSource,
          body,
          fijiSink(fijiTable.getURI),
          validationRow = "row1",
          validationColumns = Seq(FijiColumnName.create("family", "column2")),
          expectedSet = Array("v1", "v2", "v3", "v4").map { s => new Utf8(s) }.toSet)
    }
    finally {
      fijiTable.release()
    }
  }

  @junit.Test
  def testMultipleWriteSameVersion(): Unit =  {

    val fijiTable: FijiTable = setupFijiTableForTest()

    new InstanceBuilder(fiji)
        .withTable(fijiTable)
        .withRow("row1")
            .withFamily("family")
                .withQualifier("column1")
                    .withValue(1l, "v1")
                    .withValue(1l, "v2")
                    .build()

    def body(source: TypedPipe[ExpressResult]): TypedPipe[Seq[ExpressColumnOutput[Utf8]]] = {
      source.map { row: ExpressResult =>
        row.qualifiedColumnCells[Utf8]("family", "column1")
          .map {
          cell: FlowCell[Utf8] =>
            ExpressColumnOutput(
                EntityId("row1"),
                "family",
                "column2",
                cell.datum,
                version = Some(cell.version))
        }.toSeq
      }
    }

    try{
      runTypedTest[Utf8](
          fijiTable,
          defaultTestColumnInputSource,
          body,
          fijiSink(fijiTable.getURI),
          validationRow = "row1",
          validationColumns = Seq(FijiColumnName.create("family", "column2")),
          expectedSet = Set(new Utf8("v2")))
    }
    finally {
      fijiTable.release()
    }
  }

  @junit.Test
  def testReadWriteAvro(): Unit = {

    val fijiTable: FijiTable = setupFijiTableForTest()
    val simpleRecord1 = SimpleRecord.newBuilder().setL(4L).setS("4").build()
    val simpleRecord2 = SimpleRecord.newBuilder().setL(5L).setS("5").build()

    new InstanceBuilder(fiji)
        .withTable(fijiTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column3")
                        .withValue(1l, simpleRecord1)
                        .withValue(2l, simpleRecord2)
                    .build()

    def body(
        source: TypedPipe[ExpressResult]
    ): TypedPipe[Seq[ExpressColumnOutput[SimpleRecord]]] = {
        source.map { result: ExpressResult =>
          result.qualifiedColumnCells("family", "column3").map { cell : FlowCell[SimpleRecord] =>
            ExpressColumnOutput(
                EntityId("row1"),
                "family",
                "column4",
                cell.datum,
                SchemaSpec.Specific(classOf[SimpleRecord]),
                version = Some(cell.version))
          }.toSeq
        }
    }

    try{
      runTypedTest[SimpleRecord](
          fijiTable,
          defaultTestColumnInputSource,
          body,
          fijiSink(fijiTable.getURI),
          validationRow = "row1",
          validationColumns = Seq(FijiColumnName.create("family", "column4")),
          expectedSet = Set(
              SimpleRecord.newBuilder().setL(4L).setS("4").build(),
              SimpleRecord.newBuilder().setL(5L).setS("5").build()))
    }
    finally {
      fijiTable.release()
    }
  }
}
