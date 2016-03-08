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

import java.io.File

import com.twitter.scalding.Args
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tool
import com.twitter.scalding.Tsv
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import com.moz.fiji.express.flow.ColumnFamilyOutputSpec
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.FijiJob
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.mapreduce.HFileLoader
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiDataRequestBuilder
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.layout.FijiTableLayouts
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest

class FijiJobIntegration extends AbstractFijiIntegrationTest {

  private var mFiji: Fiji = null

  @Before
  def setupTest {
    val desc = FijiTableLayouts.getLayout("layout/avro-types-1.3.json")
    mFiji = Fiji.Factory.open(getFijiURI())
    mFiji.createTable(desc)
  }

  @After
  def cleanupTest {
    mFiji.release()
  }

  @Test
  def testShouldBulkLoadMapReduceJob {
    ResourceUtil.withFijiTable(mFiji, "table") { table =>
      val tempHFileFolder = mTempDir.newFolder()
      FileUtils.deleteDirectory(tempHFileFolder)

      val toolRunnerArgs = Array(
        classOf[SimpleAverageJob].getName(),
        "--input",
        "src/test/resources/data/input_lines.txt",
        "--output",
        table.getURI().toString(),
        "--hfile-output",
        tempHFileFolder.toString(),
        "--hdfs")

      Tool.main(toolRunnerArgs)

      bulkLoad(tempHFileFolder, table)

      ResourceUtil.withFijiTableReader(table) { myReader =>
        val request = FijiDataRequest.create("family", "double_column")
        val result = myReader.get(table.getEntityId("key1"), request)

        Assert.assertEquals(20.0, result.getMostRecentValue("family", "double_column"), 0.0d)
      }
    }
  }

  @Test
  def testShouldBulkLoadMapOnlyJob {
    ResourceUtil.withFijiTable(mFiji, "table") { table =>
      val tempHFileFolder = mTempDir.newFolder()
      FileUtils.deleteDirectory(tempHFileFolder)

      val toolRunnerArgs = Array(
        classOf[SimpleLoaderJob].getName(),
        "--input",
        "src/test/resources/data/input_lines.txt",
        "--output",
        table.getURI().toString(),
        "--hfile-output",
        tempHFileFolder.toString(),
        "--hdfs")

      Tool.main(toolRunnerArgs)

      bulkLoad(tempHFileFolder, table)

      ResourceUtil.withFijiTableReader(table) { myReader =>
        val colBuilder = FijiDataRequestBuilder.ColumnsDef
          .create()
          .withMaxVersions(10).add("family", "double_column")

        val request = FijiDataRequest.builder().addColumns(colBuilder).build()
        val result = myReader.get(table.getEntityId("key1"), request)
        val cells = result.getCells("family", "double_column")

        Assert.assertEquals(3, cells.size())
        Assert.assertEquals(30.0, result.getMostRecentValue("family", "double_column"), 0.0d)
      }
    }
  }

  @Test
  def testShouldBulkLoadMapOnlyJobWithAnotherOutput {
    ResourceUtil.withFijiTable(mFiji, "table") { table =>
      val tempHFileFolder = mTempDir.newFolder()
      FileUtils.deleteDirectory(tempHFileFolder)

      val tempTsvFolder = mTempDir.newFolder()
      FileUtils.deleteDirectory(tempTsvFolder)

      val toolRunnerArgs = Array(
        classOf[SimpleLoaderMultiOutputJob].getName(),
        "--input",
        "src/test/resources/data/input_lines.txt",
        "--output",
        table.getURI().toString(),
        "--tsv_output",
        tempTsvFolder.toString(),
        "--hfile-output",
        tempHFileFolder.toString(),
        "--hdfs")

      Tool.main(toolRunnerArgs)

      bulkLoad(tempHFileFolder, table)

      ResourceUtil.withFijiTableReader(table) { myReader =>
        val colBuilder = FijiDataRequestBuilder.ColumnsDef
          .create()
          .withMaxVersions(10).add("family", "double_column")

        val request = FijiDataRequest.builder().addColumns(colBuilder).build()
        val result = myReader.get(table.getEntityId("key1"), request)
        val cells = result.getCells("family", "double_column")

        Assert.assertEquals(3, cells.size())
        Assert.assertEquals(30.0, result.getMostRecentValue("family", "double_column"), 0.0d)
      }
    }
  }

  @Test
  def testShouldBulkLoadIntoMapFamily {
    ResourceUtil.withFijiTable(mFiji, "table") { table =>
      val tempHFileFolder = mTempDir.newFolder()
      FileUtils.deleteDirectory(tempHFileFolder)

      val tempTsvFolder = mTempDir.newFolder()
      FileUtils.deleteDirectory(tempTsvFolder)

      val toolRunnerArgs = Array(
        classOf[SimpleLoaderMapTypeFamilyJob].getName(),
        "--input",
        "src/test/resources/data/input_lines.txt",
        "--output",
        table.getURI().toString(),
        "--hfile-output",
        tempHFileFolder.toString(),
        "--hdfs")

      Tool.main(toolRunnerArgs)

      bulkLoad(tempHFileFolder, table)

      ResourceUtil.withFijiTableReader(table) { myReader =>
        val colBuilder = FijiDataRequestBuilder.ColumnsDef
          .create()
          .withMaxVersions(10).addFamily("searches_dev")

        val request = FijiDataRequest.builder().addColumns(colBuilder).build()
        val result = myReader.get(table.getEntityId("key1"), request)
        val cells = result.getCells("searches_dev")

        Assert.assertEquals(3, cells.size())
      }
    }
  }

  private def bulkLoad(hFilePath: File, table: FijiTable) {
    val hFileLoader = HFileLoader.create(super.getConf())
    hFileLoader.load(new Path(hFilePath.toString()), table)
  }
}

class SimpleAverageJob(args: Args) extends FijiJob(args) {

  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")
  val hFileOutput = args("hfile-output")

  // Read each line. Split on " " which should yield string, value
  // string part eventually is the entity_id, value will be averaged in the end.

  TextLine(inputUri)
    .map('line -> ('entityId, 'numViews)) { line: String =>
      val parts = line.split(" ")
      (EntityId(parts(0)), parts(1).toInt)
    }
    .groupBy('entityId) { _.average('numViews) }
    .write(HFileFijiOutput.builder
        .withTableURI(outputUri)
        .withHFileOutput(hFileOutput)
        .withColumns('numViews -> "family:double_column")
        .build)
}

class SimpleLoaderJob(args: Args) extends FijiJob(args) {

  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")
  val hFileOutput = args("hfile-output")

  // Read each line. Generate an entityId and numViews. The entityId here is duplicated
  // so there should be multiple versions of each in HBase.
  TextLine(inputUri)
    .read
    .mapTo('line -> ('entityId, 'numViews, 'ts)) { line: String =>
      val parts = line.split(" ")
      Thread.sleep(2) // Force a sleep so that we get unique timestamps
      (EntityId(parts(0)), parts(1).toDouble, System.currentTimeMillis())
    }
    .write(HFileFijiOutput.builder
        .withTableURI(outputUri)
        .withHFileOutput(hFileOutput)
        .withTimestampField('ts)
        .withColumns('numViews -> "family:double_column")
        .build)
}

class SimpleLoaderMapTypeFamilyJob(args: Args) extends FijiJob(args) {

  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")
  val hFileOutput = args("hfile-output")

  @transient
  lazy val outputCols = Map('numViews -> ColumnFamilyOutputSpec("searches_dev",
      qualifierSelector='numViews))

  // Read each line. Generate an entityId and numViews. The entityId here is duplicated
  // so there should be multiple versions of each in HBase.
  TextLine(inputUri)
    .read
    .mapTo('line -> ('entityId, 'numViews, 'ts)) { line: String =>
      val parts = line.split(" ")
      Thread.sleep(2) // Force a sleep so that we get unique timestamps
      (EntityId(parts(0)), parts(1).toInt, System.currentTimeMillis())
    }
    .write(HFileFijiOutput.builder
        .withTableURI(outputUri)
        .withHFileOutput(hFileOutput)
        .withTimestampField('ts)
        .withColumnSpecs(outputCols)
        .build)
}

class SimpleLoaderMultiOutputJob(args: Args) extends FijiJob(args) {

  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")
  val tsvOutputURI: String = args("tsv_output")
  val hFileOutput = args("hfile-output")

  // Read each line. Generate an entityId and numViews. The entityId here is duplicated
  // so there should be multiple versions of each in HBase.
  val computePipe = TextLine(inputUri)
    .read
    .mapTo('line -> ('entityId, 'numViews, 'ts)) { line: String =>
      val parts = line.split(" ")
      Thread.sleep(2) // Force a sleep so that we get unique timestamps
      (EntityId(parts(0)), parts(1).toDouble, System.currentTimeMillis())
    }

  computePipe.write(HFileFijiOutput.builder
      .withTableURI(outputUri)
      .withHFileOutput(hFileOutput)
      .withTimestampField('ts)
      .withColumns('numViews -> "family:double_column")
      .build)
  computePipe.write(Tsv(tsvOutputURI))
}
