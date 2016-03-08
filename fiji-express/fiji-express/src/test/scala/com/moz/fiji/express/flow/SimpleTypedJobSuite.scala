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

import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TypedTsv
import org.apache.avro.util.Utf8
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.FijiSuite
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.layout.FijiTableLayouts
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.express.flow.util.TestingResourceUtil


class AnagramCountJob(args: Args) extends FijiJob(args) {

  new TypedFijiSource[ExpressResult](
      args("input"),
      TimeRangeSpec.All,
      List(QualifiedColumnInputSpec
          .builder
          .withColumn("family", "column1")
          .withMaxVersions(7).build
      )
  )
  .flatMap { entry: ExpressResult =>
    entry.qualifiedColumnCells[Utf8]("family", "column1").map {
      cell: FlowCell[Utf8] => cell.datum.toString.sorted
    }
  }.groupBy { word: String => word}
  .size
  .toTypedPipe
  .write(TypedTsv[(String, Long)](args("output")))
}

@RunWith(classOf[JUnitRunner])
class SimpleTypedJobSuite extends FijiSuite {

  val tableLayout: FijiTableLayout = TestingResourceUtil.layout(FijiTableLayouts.SIMPLE_TWO_COLUMNS)
  val fijiTable: FijiTable = makeTestFijiTable(tableLayout)

  test("A simple type safe fiji express job that counts the number of anagrams from a given " +
      "set of words.") {

    ResourceUtil.doAndRelease(fijiTable) {
      table: FijiTable =>
        val uri = table.getURI.toString
        val anagramCountInput = fijiRowDataSlice[String](
            fijiTable,
            "row01",
            "family:column1",
            (1L, "teba"), (2L, "alpah"), (3L, "alpha"),
            (4L, "beta"), (5L, "alaph"), (6L, "gamma"), (7L, "beta"))

        // Method to validate output.
        def validateOutput(outputBuffer: Buffer[(String, Long)]) {
          val bufferMap = outputBuffer.toMap
          assert(bufferMap("aagmm") == 1l)
          assert(bufferMap("aahlp") == 3l)
          assert(bufferMap("abet") == 3l)
        }

        JobTest(new AnagramCountJob(_))
            .arg("input", uri)
            .arg("output", "outputFile")
            .source(
                new TypedFijiSource[ExpressResult](
                      uri,
                      TimeRangeSpec.All,
                      List(QualifiedColumnInputSpec.builder.withColumn("family", "column1").build)),
                  anagramCountInput)
            .sink(TypedTsv[(String, Long)]("outputFile"))(validateOutput)
            .run
            .finish
    }
  }
}
