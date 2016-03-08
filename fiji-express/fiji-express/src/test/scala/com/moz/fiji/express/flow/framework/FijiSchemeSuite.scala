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

package com.moz.fiji.express.flow.framework

import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.FijiOutput
import com.moz.fiji.express.flow.FijiSource
import com.moz.fiji.express.flow.QualifiedColumnOutputSpec
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.layout.FijiTableLayouts

@RunWith(classOf[JUnitRunner])
class FijiSchemeSuite extends FijiSuite {
  test("A FijiJob can write to a fully qualified column in a column family.") {
    val layout = FijiTableLayout.newLayout(
      FijiTableLayouts.getLayout("layout/avro-types-1.3.json"))
    val testTable = makeTestFijiTable(layout)
    val tableUri = testTable.getURI.toString
    val input = List((0, "1"))

    def validateOutput(b: Buffer[Any]): Unit = {
      println(b)
      b === Seq(1)
    }

    JobTest(new FijiSchemeSuite.IdentityJob(_))
      .arg("input", "temp")
      .arg("output", tableUri)
      .source(TextLine("temp"), input)
      .sink(FijiSchemeSuite.output(tableUri)) (validateOutput)
      .runHadoop
  }
}

object FijiSchemeSuite {
  // Construct the FijiOutput used in IdentityJob, given a table URI.
  def output(uri: String): FijiSource = FijiOutput.builder
    .withTableURI(uri)
    .withColumnSpecs(Map('line -> QualifiedColumnOutputSpec.builder
    .withFamily("searches").withQualifier("dummy-qualifier").build)).build

  class IdentityJob(args: Args) extends Job(args) {
    TextLine(args("input"))
        .map('offset -> 'entityId) {offset: Int => EntityId(offset.toString)}
        .map('line -> 'line) { line: String => line.toInt }
        .write(output(args("output")))
  }
}
