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

package com.moz.fiji.express.flow

import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.avro.SimpleRecord
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.express.flow.util.TestingResourceUtil
import com.moz.fiji.express.FijiSuite
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.layout.FijiTableLayout

@RunWith(classOf[JUnitRunner])
class FijiSourceSuite
    extends FijiClientTest
    with FijiSuite
    with BeforeAndAfter {
  import FijiSourceSuite._

  /* Undo all changes to hdfs mode. */
  before {
    val nilArgsWithMode = Mode.putMode(Local(strictSources = true), Args(Nil))
  }

  after {
    val nilArgsWithMode = Mode.putMode(Local(strictSources = true), Args(Nil))
  }

  setupFijiTest()

  /** Table layout using Avro schemas to use for tests. The row keys are formatted. */
  val avroLayout: FijiTableLayout = TestingResourceUtil.layout("layout/avro-types.json")

  // TODO: Tests below this line still use JobTest and should be rewritten.
  test("A job that writes using the generic API is run.") {
    // Create test Fiji table.
    val uri: String = ResourceUtil.doAndRelease(makeTestFijiTable(avroLayout)) { table: FijiTable =>
      table.getURI.toString
    }

    // Input to use with Text source.
    val genericWriteInput: List[(String, String)] = List(
      ( "0", "zero" ),
      ( "1", "one" ))

    // Validates the output buffer contains the same as the input buffer.
    def validateGenericWrite(outputBuffer: Buffer[(EntityId, Seq[FlowCell[GenericRecord]])]) {
      val inputMap: Map[Long, String] = genericWriteInput.map { t => t._1.toLong -> t._2 }.toMap
      outputBuffer.foreach { t: (EntityId, Seq[FlowCell[GenericRecord]]) =>
        val entityId = t._1
        val record = t._2.head.datum

        val s = record.get("s").asInstanceOf[String]
        val l = record.get("l").asInstanceOf[Long]

        assert(entityId(0) === s)
        assert(inputMap(l) === s)
      }
    }

    val jobTest = JobTest(new GenericAvroWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), genericWriteInput)
      .sink(FijiOutput.builder
      .withTableURI(uri)
      .withColumns('record -> "family:column4")
      .build
      )(validateGenericWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }


  test ("A job that writes to map-type column families is run.") {
    // URI of the Fiji table to use.
    val uri: String = ResourceUtil.doAndRelease(makeTestFijiTable(avroLayout)) { table: FijiTable =>
      table.getURI.toString
    }

    // Input text.
    val mapTypeInput: List[(String, String)] = List(
        ("0", "dogs 4"),
        ("1", "cats 5"),
        ("2", "fish 3"))

    // Validate output.
    def validateMapWrite(
        outputBuffer: Buffer[(EntityId,Seq[FlowCell[Int]])]
    ) {
      assert (1 === outputBuffer.size)
      val outputSlice = outputBuffer(0)._2
      val outputSliceMap = outputSlice.groupBy(_.qualifier)
      assert (4 === outputSliceMap("dogs").head.datum)
      assert (5 === outputSliceMap("cats").head.datum)
      assert (3 === outputSliceMap("fish").head.datum)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapWriteJob(_))
        .arg("input", "inputFile")
        .arg("table", uri)
        .source(TextLine("inputFile"), mapTypeInput)
        .sink(FijiOutput.builder
            .withTableURI(uri)
            .withColumnSpecs('resultCount -> ColumnFamilyOutputSpec.builder
                .withFamily("searches")
                .withQualifierSelector('terms)
                .build)
            .build
        )(validateMapWrite)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test ("A job that writes to map-type column families with numeric column qualifiers is run.") {
    // URI of the Fiji table to use.
    val uri: String = ResourceUtil.doAndRelease(makeTestFijiTable(avroLayout)) { table: FijiTable =>
      table.getURI.toString
    }

    // Create input using mapSlice.
    val mapTypeInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 3)
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert (outputSet.contains("0 dogs"), "Failed on \"0 dogs\" test")
      assert (outputSet.contains("1 cat"), "Failed on \"1 cat\" test")
      assert (outputSet.contains("2 fish"), "Failed on \"2 fish\" test")
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapSliceJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(FijiInput.builder
            .withTableURI(uri)
            .withColumns("animals" -> 'terms)
            .build, mapTypeInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A job that joins two pipes, on string keys, is run in both local and hadoop mode.") {
    // URI of the Fiji table to use.
    val uri: String = ResourceUtil.doAndRelease(makeTestFijiTable(avroLayout)) { table: FijiTable =>
      table.getURI.toString
    }

    // Create input from Fiji table.
    val joinFijiInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Create input from side data.
    val sideInput: List[(String, String)] = List( ("0", "0row"), ("1", "2row") )

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinOnStringsJob(_))
        .arg("input", uri)
        .arg("side-input", "sideInputFile")
        .arg("output", "outputFile")
        .source(FijiInput.builder
            .withTableURI(uri)
            .withColumns("animals" -> 'animals)
            .build, joinFijiInput)
        .source(TextLine("sideInputFile"), sideInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test in local mode.
    jobTest.run.finish

    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }
}

/** Companion object for FijiSourceSuite. Contains test jobs. */
object FijiSourceSuite {
  /**
   * A job that uses the generic API, creating a record containing the text from the input,
   * and writing it to a Fiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Fiji table.
   */
  class GenericAvroWriteJob(args: Args) extends FijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
      .read
      .map('offset -> 'timestamp) { offset: String => offset.toLong }
      .map('offset -> 'l) { offset: String => offset.toLong }
      // Generate an entityId for each line.
      .map('line -> 'entityId) { EntityId(_: String) }
      .rename('line -> 's)
      .packGenericRecord(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
      // Write the results to the "family:column4" column of a Fiji table.
      .project('entityId, 'record)
      .write(FijiOutput.builder
      .withTableURI(args("output"))
      .withColumns('record -> "family:column4")
      .build)

    override def config: Map[AnyRef, AnyRef] = {
      val superConfig = super.config

      // make sure that things are configured correctly here.
      require(superConfig.contains(com.twitter.chill.config.ConfiguredInstantiator.KEY))
      require(
          superConfig(com.twitter.chill.config.ConfiguredInstantiator.KEY)
              == classOf[com.moz.fiji.express.flow.framework.serialization.FijiKryoInstantiator].getName
      )

      superConfig
    }
  }


  /**
   * A job that writes to a map-type column family.  It takes text from the input and uses it as
   * search terms and the number of results returned for that term. All of them belong to the same
   * entity, "my_eid".
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Fiji table.
   */
  class MapWriteJob(args: Args) extends FijiJob(args) {
    TextLine(args("input"))
        .read
        // Create an entity ID for each line (always the same one, here)
        .map('line -> 'entityId) { line: String => EntityId("my_eid") }
        // new  the number of result for each search term
        .map('line -> ('terms, 'resultCount)) { line: String =>
          (line.split(" ")(0), line.split(" ")(1).toInt)
        }
        // Write the results to the "family:column1" column of a Fiji table.
        .write(FijiOutput.builder
            .withTableURI(args("table"))
            .withColumnSpecs('resultCount -> ColumnFamilyOutputSpec.builder
                .withFamily("searches")
                .withQualifierSelector('terms)
                .build
            )
            .build)
  }

  /**
   * A job that tests map-type column families using sequences of cells and outputs the results to
   * a TSV.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Fiji table, and "output", which specifies the path to a text file.
   */
  class MapSliceJob(args: Args) extends FijiJob(args) {
    FijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'terms)
        .build
        .map('terms -> 'values) { terms: Seq[FlowCell[CharSequence]] => terms.head.datum }
        .project('values)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, on String keys.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Fiji table, and "output", which specifies the path to a text file.
   */
  class JoinOnStringsJob(args: Args) extends FijiJob(args) {
    val sidePipe = TextLine(args("side-input"))
        .read
        .map('line -> 'entityId) { line: String => EntityId(line) }

    FijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'animals)
        .build
        .map('animals -> 'terms) { animals: Seq[FlowCell[CharSequence]] =>
          animals.head.datum.toString.split(" ")(0) + "row" }
        .discard('entityId)
        .joinWithSmaller('terms -> 'line, sidePipe)
        .write(Tsv(args("output")))
  }
}
