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

package com.moz.fiji.express.flow.framework.hfile

import scala.collection.JavaConverters.asScalaIteratorConverter

import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import com.moz.fiji.express.IntegrationUtil._
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.FijiJob
import com.moz.fiji.express.flow.FijiOutput
import com.moz.fiji.express.flow.framework.FijiScheme
import com.moz.fiji.express.flow.util.ResourceUtil._
import com.moz.fiji.express.flow.util.{AvroTypesComplete => ATC}
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest

class IntegrationTestHFileOutput extends AbstractFijiIntegrationTest {
  import IntegrationTestHFileOutput._

  private var fiji: Fiji = null
  private var conf: Configuration = null

  @Before
  def setupTest(): Unit = {
    fiji = Fiji.Factory.open(getFijiURI())
    conf = getConf()
  }

  @After
  def cleanupTest(): Unit = fiji.release()

  @Test
  def testShouldBulkLoadHFiles(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    Assert.assertTrue(conf.get("mapred.output.dir").startsWith(conf.get("hadoop.tmp.dir")))
    FileSystem.get(conf).delete(new Path(hfileOutput), true)

    fiji.createTable(ATC.layout.getDesc)

    doAndRelease(fiji.openTable(ATC.name)) { table =>
      runJob(classOf[HFileOutputMapOnly],
        "--hdfs", "--table-uri", table.getURI.toString, "--hfile-output", hfileOutput)

      bulkLoadHFiles(hfileOutput + "/hfiles", conf, table)

      validateInts(table)
    }
  }

  @Test
  def testShouldBulkLoadWithReducer(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    fiji.createTable(ATC.layout.getDesc)

    doAndRelease(fiji.openTable(ATC.name)) { table =>
      runJob(classOf[HFileOutputWithReducer],
        "--hdfs", "--table-uri", table.getURI.toString, "--hfile-output", hfileOutput)

      bulkLoadHFiles(hfileOutput + "/hfiles", conf, table)

      validateCount(table)
    }
  }

  @Test
  def testShouldBulkLoadMultipleTables(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"
    val bOutput = hfileOutput + "/b"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layoutA = ATC.layout.getDesc
    layoutA.setName("A")
    fiji.createTable(layoutA)

    val layoutB = ATC.layout.getDesc
    layoutB.setName("B")
    fiji.createTable(layoutB)

    doAndRelease(fiji.openTable("A")) { a: FijiTable =>
      doAndRelease(fiji.openTable("B")) { b: FijiTable =>
        runJob(classOf[HFileOutputMultipleTables],
          "--hdfs",
          "--a", a.getURI.toString, "--b", b.getURI.toString,
          "--a-output", aOutput, "--b-output", bOutput)

        bulkLoadHFiles(aOutput + "/hfiles", conf, a)
        bulkLoadHFiles(bOutput + "/hfiles", conf, b)

        validateInts(a)
        validateCount(b)
      }
    }
  }

  @Test
  def testShouldBulkLoadHFileAndDirect(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layoutA = ATC.layout.getDesc
    layoutA.setName("A")
    fiji.createTable(layoutA)

    val layoutB = ATC.layout.getDesc
    layoutB.setName("B")
    fiji.createTable(layoutB)

    doAndRelease(fiji.openTable("A")) { a: FijiTable =>
      doAndRelease(fiji.openTable("B")) { b: FijiTable =>
        runJob(classOf[HFileOutputAndDirectOutput],
          "--hdfs",
          "--a", a.getURI.toString, "--b", b.getURI.toString,
          "--a-output", aOutput)

        bulkLoadHFiles(aOutput + "/hfiles", conf, a)

        validateInts(a)
        validateInts(b)
      }
    }
  }

  @Test
  def testShouldBulkLoadMultipleHFilesToOneTable(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"
    val bOutput = hfileOutput + "/b"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layout = ATC.layout.getDesc
    fiji.createTable(layout)

    doAndRelease(fiji.openTable(layout.getName)) { table: FijiTable =>
      runJob(classOf[HFileOuputMultipleToSameTable],
        "--hdfs",
        "--uri", table.getURI.toString,
        "--a-output", aOutput, "--b-output", bOutput)

      bulkLoadHFiles(aOutput + "/hfiles", conf, table)
      bulkLoadHFiles(bOutput + "/hfiles", conf, table)

      validateInts(table)
      validateCount(table)
    }
  }
}

object IntegrationTestHFileOutput {
  val count: Int = 100
  val inputs: Set[(EntityId, Int)] =
    ((1 to count).map(int => EntityId(int.toString)) zip (1 to count)).toSet
  val countEid: EntityId = EntityId("count")

  def validateInts(table: FijiTable): Unit = withFijiTableReader(table) { reader =>
    val request = FijiDataRequest.create(ATC.family, ATC.intColumn)
    doAndClose(reader.getScanner(request)) { scanner =>
      val outputs = for (rowData <- scanner.iterator().asScala)
      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
            rowData.getMostRecentValue(ATC.family, ATC.intColumn))
      Assert.assertEquals(inputs, outputs.toSet)
    }
  }

  def validateCount(table: FijiTable): Unit = withFijiTableReader(table) { reader =>
    val request = FijiDataRequest.create(ATC.family, ATC.longColumn)
    doAndClose(reader.getScanner(request)) { scanner =>
      val outputs = for (rowData <- scanner.iterator().asScala)
      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
            rowData.getMostRecentValue(ATC.family, ATC.longColumn))
      Assert.assertEquals(List(countEid -> count), outputs.toList)
    }
  }
}

class HFileOutputMapOnly(args: Args) extends FijiJob(args) {
  import IntegrationTestHFileOutput._
  val uri = args("table-uri")
  val hfilePath = args("hfile-output")

  IterableSource(inputs, (FijiScheme.EntityIdField, 'int))
    .read
    .write(HFileFijiOutput.builder
        .withTableURI(uri)
        .withHFileOutput(hfilePath)
        .withColumns('int -> (ATC.family +":"+ ATC.intColumn))
        .build)
}

class HFileOutputWithReducer(args: Args) extends FijiJob(args) {
  import IntegrationTestHFileOutput._
  val uri = args("table-uri")
  val hfilePath = args("hfile-output")

  IterableSource(inputs, (FijiScheme.EntityIdField, 'int))
      .read
      .groupAll { _.size }
      .insert('entityId, countEid)
      .write(HFileFijiOutput.builder
          .withTableURI(uri)
          .withHFileOutput(hfilePath)
          .withColumns('size -> (ATC.family +":"+ ATC.longColumn))
          .build)
}

class HFileOutputMultipleTables(args: Args) extends FijiJob(args) {
  import IntegrationTestHFileOutput._
  val aUri = args("a")
  val bUri = args("b")
  val aOutput = args("a-output")
  val bOutput = args("b-output")

  val pipe = IterableSource(inputs, (FijiScheme.EntityIdField, 'int)).read

  pipe.write(HFileFijiOutput.builder
      .withTableURI(aUri)
      .withHFileOutput(aOutput)
      .withColumns('int -> (ATC.family +":"+ ATC.intColumn))
      .build)
  pipe
    .groupAll { _.size }
    .insert('entityId, countEid)
    .write(HFileFijiOutput.builder
        .withTableURI(bUri)
        .withHFileOutput(bOutput)
        .withColumns('size -> (ATC.family +":"+ ATC.longColumn))
        .build)
}

class HFileOutputAndDirectOutput(args: Args) extends FijiJob(args) {
  import IntegrationTestHFileOutput._
  val aUri = args("a")
  val bUri = args("b")
  val aOutput = args("a-output")

  val pipe = IterableSource(inputs, (FijiScheme.EntityIdField, 'int)).read

  pipe.write(HFileFijiOutput.builder
      .withTableURI(aUri)
      .withHFileOutput(aOutput)
      .withColumns('int -> (ATC.family +":"+ ATC.intColumn))
      .build)
  pipe.write(
      FijiOutput
        .builder
        .withTableURI(bUri)
        .withColumns('int -> (ATC.family +":"+ ATC.intColumn))
        .build)
}

class HFileOuputMultipleToSameTable(args: Args) extends FijiJob(args) {
  import IntegrationTestHFileOutput._
  val uri = args("uri")
  val aOutput = args("a-output")
  val bOutput = args("b-output")

  val pipe = IterableSource(inputs, (FijiScheme.EntityIdField, 'int)).read

  pipe.write(HFileFijiOutput.builder
      .withTableURI(uri)
      .withHFileOutput(aOutput)
      .withColumns('int -> (ATC.family +":"+ ATC.intColumn))
      .build)
  pipe
      .groupAll { _.size }
      .insert('entityId, countEid)
      .write(HFileFijiOutput.builder
          .withTableURI(uri)
          .withHFileOutput(bOutput)
          .withColumns('size -> (ATC.family +":"+ ATC.longColumn))
          .build)
}
