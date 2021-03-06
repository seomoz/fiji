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

import scala.collection.mutable

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.FijiJobSuiteSampleData
import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.avro.SimpleRecord
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI

@RunWith(classOf[JUnitRunner])
class FijiJobSuite extends FijiSuite {
  import FijiJobSuiteSampleData._

  val uri: String = ResourceUtil.doAndRelease(makeTestFijiTable(avroLayout)) { table: FijiTable =>
    table.getURI.toString
  }

  def validateUnpacking(output: mutable.Buffer[(Long, String, String)]): Unit = {
    val inputMap = rawInputs.toMap
    output.foreach { case (l: Long, s: String, o: String) =>
      assert(inputMap(l) === s)
      assert("default-value" === o)
    }
  }

  test("A FijiJob can pack a generic Avro record.") {
    def validatePacking(outputs: mutable.Buffer[(EntityId, Seq[FlowCell[GenericRecord]])]) {
      val inputMap = rawInputs.toMap
      outputs.foreach { case (_: EntityId, slice: Seq[FlowCell[GenericRecord]]) =>
        val record = slice.head.datum
        assert(inputMap(record.get("l").asInstanceOf[Long]) === record.get("s"))
        assert("default-value" === record.get("o"))
      }
    }

    val jobTest = JobTest(new PackGenericRecordJob(_))
        .arg("input", "inputFile")
        .arg("uri", uri)
        .source(Tsv("inputFile", fields = new Fields("l", "s")), rawInputs)
        .sink(FijiOutput.builder
            .withTableURI(uri)
            .withColumns('record -> "family:simple")
            .build
        )(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A FijiJob can pack a specific Avro record.") {
    def validatePacking(outputs: mutable.Buffer[(EntityId, Seq[FlowCell[SimpleRecord]])]) {
      val inputMap = rawInputs.toMap
      outputs.foreach { case (_: EntityId, slice: Seq[FlowCell[SimpleRecord]]) =>
        val record = slice.head.datum
        assert(inputMap(record.getL) === record.getS)
        assert("default-value" === record.getO)
      }
    }

    val jobTest = JobTest(new PackSpecificRecordJob(_))
        .arg("input", "inputFile")
        .arg("uri", uri)
        .source(Tsv("inputFile", fields = new Fields("l", "s")), rawInputs)
        .sink(FijiOutput.builder
            .withTableURI(uri)
            .withColumnSpecs('record -> QualifiedColumnOutputSpec.builder
                .withFamily("family")
                .withQualifier("simple")
                .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
                .build)
            .build
        )(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A FijiJob can unpack a generic record.") {
    val slices: List[Seq[FlowCell[GenericRecord]]] = genericInputs.map { record: GenericRecord =>
      List(FlowCell("family", "simple", datum = record))
    }
    val input: List[(EntityId, Seq[FlowCell[GenericRecord]])] = eids.zip(slices)

    val jobTest = JobTest(new UnpackGenericRecordJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(FijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("family", "simple")
                .withSchemaSpec(SchemaSpec.Generic(SimpleRecord.getClassSchema))
                .build -> 'slice)
            .build, input)
        .sink(Tsv("outputFile"))(validateUnpacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A FijiJob can unpack a specific record.") {
    val slices: List[Seq[FlowCell[SpecificRecord]]] = specificInputs
        .map { record: SpecificRecord =>
          List(FlowCell("family", "simple", datum = record))
        }
    val input: List[(EntityId, Seq[FlowCell[SpecificRecord]])] = eids.zip(slices)

    val jobTest = JobTest(new UnpackSpecificRecordJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(FijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("family", "simple")
                .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
                .build -> 'slice)
            .build, input)
        .sink(Tsv("outputFile"))(validateUnpacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A FijiJob is not run if the Fiji instance in the output doesn't exist.") {
    class BasicJob(args: Args) extends FijiJob(args) {
      TextLine(args("input"))
        .map ('line -> 'entityId) { line: String => EntityId(line) }
        .write(FijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('line -> "family:column1")
            .build)
    }

    val nonexistentInstanceURI: String = FijiURI.newBuilder(uri)
        .withInstanceName("nonexistent_instance")
        .build()
        .toString

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", nonexistentInstanceURI)
        .source(TextLine("inputFile"), basicInput)
        .sink(FijiOutput.builder
            .withTableURI(nonexistentInstanceURI)
            .withColumns('line -> "family:column1")
            .build
        )(validateBasicJob)

    val hadoopException = intercept[InvalidFijiTapException] { jobTest.runHadoop.finish }
    val localException = intercept[InvalidFijiTapException] { jobTest.run.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getCause.getMessage.contains("nonexistent_instance"))
  }

  test("A FijiJob is not run if the Fiji table in the output doesn't exist.") {
    class BasicJob(args: Args) extends FijiJob(args) {
      TextLine(args("input"))
        .write(FijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('line -> "family:column1")
            .build)
    }

    val nonexistentTableURI: String = FijiURI.newBuilder(uri)
        .withTableName("nonexistent_table")
        .build()
        .toString

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", nonexistentTableURI)
        .source(TextLine("inputFile"), basicInput)
        .sink(FijiOutput.builder
            .withTableURI(nonexistentTableURI)
            .withColumns('line -> "family:column1")
            .build
        )(validateBasicJob)

    val localException = intercept[InvalidFijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidFijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_table"))
  }

  test("A FijiJob is not run if any of the columns don't exist.") {
    class BasicJob(args: Args) extends FijiJob(args) {
      TextLine(args("input"))
        .write(FijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('line -> "family:nonexistent_column")
            .build)
    }

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), basicInput)
        .sink(FijiOutput.builder
            .withTableURI(uri)
            .withColumns('line -> "family:nonexistent_column")
            .build
        )(validateBasicJob)

    val localException = intercept[InvalidFijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidFijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_column"))
  }

  test("FijiJob adds the classpath to the tmpjars property by default.") {
    val testingArgs: Args =
      Mode.putMode(Mode(Args(List("--hdfs")), HBaseConfiguration.create()), Args(List("--hdfs")))
    val job: FijiJob = new FijiJob(testingArgs)
    val currentClasspathJars: Seq[String] =
      FijiJob.classpathJars().map { _.split(",").toSeq }.getOrElse(Seq[String]())
    val distJars: Seq[String] =
        job.config.getOrElse(FijiJob.tmpjarsConfigProperty, "")
            .asInstanceOf[String]
            .split(",")
    currentClasspathJars.foreach { jarLocation: String =>
      assert(distJars.contains(jarLocation), "distJars should contain: %s".format(jarLocation))
    }
  }

  test("FijiJob does not add the classpath to the tmpjars property if specified.") {
    val testingArgs: Args =
      Mode.putMode(
        Mode(Args(List("--hdfs")), HBaseConfiguration.create()),
        Args(List("--" + FijiJob.addClasspathArg, "false")))
    val job: FijiJob = new FijiJob(testingArgs)
    val distJars: String =
      job.config.getOrElse(FijiJob.tmpjarsConfigProperty, "")
          .asInstanceOf[String]
    assert(
      distJars.isEmpty,
      "There should be nothing in distJars since nothing was specified, but it is %s."
          .format(distJars))
  }

  test("FijiJob adds user-specified jars to tmpjars along with the classpath.") {
    val testFileLocation: String = "file://bogus/file/path"
    val testingArgs: Args =
      Mode.putMode(
        Mode(Args(List("--hdfs")), HBaseConfiguration.create()),
        Args(List(
          "--" + FijiJob.addClasspathArg, "true",
          "--" + FijiJob.tmpjarsArg, testFileLocation)))
    val currentClasspathJars: Seq[String] =
      FijiJob.classpathJars().map { _.split(",").toSeq }.getOrElse(Seq[String]())
    val job: FijiJob = new FijiJob(testingArgs)
    val distJars: Seq[String] =
      job.config.getOrElse(FijiJob.tmpjarsConfigProperty, "")
          .asInstanceOf[String]
          .split(",")
    assert(distJars.contains(testFileLocation), "Distjars should contain the user-specified jar.")
    currentClasspathJars.foreach { jarLocation: String =>
      assert(distJars.contains(jarLocation), "distJars should contain: %s".format(jarLocation))
    }
  }

  test("FijiJob adds user-specified jars to the tmpjars property without the classpath.") {
    val testFileLocation: String = "file://bogus/file/path"
    val testingArgs: Args =
      Mode.putMode(
        Mode(Args(List("--hdfs")), HBaseConfiguration.create()),
        Args(List(
            "--" + FijiJob.addClasspathArg, "false",
            "--" + FijiJob.tmpjarsArg, testFileLocation)))
    val job: FijiJob = new FijiJob(testingArgs)
    val distJars: Seq[String] =
      job.config.getOrElse(FijiJob.tmpjarsConfigProperty, "")
          .asInstanceOf[String]
          .split(",")
    assert(distJars.contains(testFileLocation), "Distjars should contain the user-specified jar.")
    assert(
      1 == distJars.size,
      ("Distjars should not contain anything besides the user-specified jar, " +
          "instead was: %s.").format(distJars))
  }
}

class PackGenericRecordJob(args: Args) extends FijiJob(args) {
  Tsv(args("input"), fields = ('l, 's)).read
      .packGenericRecordTo(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
      .insert('entityId, EntityId("foo"))
      .write(FijiOutput.builder
          .withTableURI(args("uri"))
          .withColumns('record -> "family:simple")
          .build)
}

class PackSpecificRecordJob(args: Args) extends FijiJob(args) {
  Tsv(args("input"), fields = ('l, 's)).read
      .packTo[SimpleRecord](('l, 's) -> 'record)
      .insert('entityId, EntityId("foo"))
      .write(FijiOutput.builder
          .withTableURI(args("uri"))
          .withColumnSpecs('record -> QualifiedColumnOutputSpec.builder
              .withFamily("family")
              .withQualifier("simple")
              .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
              .build)
          .build)
}

class UnpackGenericRecordJob(args: Args) extends FijiJob(args) {
  FijiInput.builder
      .withTableURI(args("input"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("family", "simple")
          .withSchemaSpec(SchemaSpec.Generic(SimpleRecord.getClassSchema))
          .build -> 'slice)
      .build
      .mapTo('slice -> 'record) { slice: Seq[FlowCell[GenericRecord]] => slice.head.datum }
      .unpackTo[GenericRecord]('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}

class UnpackSpecificRecordJob(args: Args) extends FijiJob(args) {
  FijiInput.builder
      .withTableURI(args("input"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("family", "simple")
          .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
          .build -> 'slice)
      .build
      .map('slice -> 'record) { slice: Seq[FlowCell[SimpleRecord]] => slice.head.datum }
      .unpackTo[SimpleRecord]('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}
