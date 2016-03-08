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

package com.moz.fiji.express.repl

import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Job
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.Tsv
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.Implicits
import com.moz.fiji.express.avro.SimpleRecord
import com.moz.fiji.express.flow.EntityId
import com.moz.fiji.express.flow.FlowCell
import com.moz.fiji.express.flow.FijiInput
import com.moz.fiji.express.flow.FijiOutput
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.express.flow.util.TestingResourceUtil
import com.moz.fiji.express.FijiSuite
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.layout.FijiTableLayouts
import com.moz.fiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class FijiPipeToolSuite extends FijiClientTest with FijiSuite {
  // Hook into FijiClientTest since methods marked with JUnit's @Before and @After annotations won't
  // run when using ScalaTest.
  setupFijiTest()

  // Create test Fiji table.
  val uri: String = {
    /** Table layout to use for tests. */
    val layout: FijiTableLayout = TestingResourceUtil.layout(FijiTableLayouts.SIMPLE_TWO_COLUMNS)

    val instanceUri = new InstanceBuilder(getFiji)
        .withTable(layout.getName, layout)
            .withRow("row01").withFamily("family").withQualifier("column1").withValue(1L, "hello")
            .withRow("row02").withFamily("family").withQualifier("column1").withValue(2L, "hello")
            .withRow("row03").withFamily("family").withQualifier("column1").withValue(1L, "world")
            .withRow("row04").withFamily("family").withQualifier("column1").withValue(3L, "hello")
        .build()
        .getURI

    val tableUri = FijiURI.newBuilder(instanceUri).withTableName(layout.getName).build()
    tableUri.toString
  }

  test("A FijiPipeTool can be used to obtain a Scalding job that is run in local mode.") {
    Implicits.mode = Local(strictSources = true)
    FijiPipeToolSuite.jobToRun(Mode.putMode(Implicits.mode, Args(Nil)), uri).run
  }

  test("A FijiPipeTool can be used to obtain a Scalding job that is run with Hadoop.") {
    Implicits.mode = Hdfs(strict = true, conf = HBaseConfiguration.create())
    FijiPipeToolSuite.jobToRun(Mode.putMode(Implicits.mode, Args(Nil)), uri).run
  }

  test("A FijiPipe can be implicitly converted to a FijiPipeTool,") {
    // Run test case in local mode so we can specify the input file.
    Implicits.mode = Local(strictSources = true)

    val tempFolder = new TemporaryFolder()
    tempFolder.create()
    val inputFile = tempFolder.newFile("input-source")

    {
      import Implicits._
      import ReplImplicits._

      // Implicitly create a FijiPipe, then call FijiPipeTool's run() method on it.
      Tsv(inputFile.getAbsolutePath, fields = ('l, 's)).read
          .packGenericRecordTo(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
          .insert('entityId, EntityId("foo"))
          .write(FijiOutput.builder.withTableURI(uri).build)
          .run()
    }
  }
}

object FijiPipeToolSuite {
  // A job obtained by converting a Cascading Pipe to a FijiPipe, which is then used to obtain
  // a Scalding Job from the pipe.
  def jobToRun(args: Args, uri: String): Job = {
    import Implicits._
    import ReplImplicits._

    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    FijiInput.builder
        .withTableURI(uri)
        .withColumns("family:column1" -> 'word)
        .build
        // Sanitize the word.
        .map('word -> 'cleanword) { words: Seq[FlowCell[CharSequence]] =>
          words.head.datum
              .toString
              .toLowerCase
        }
        // Count the occurrences of each word.
        .groupBy('cleanword) { occurences => occurences.size('count) }
        .groupAll { _.toList[(String, Int)](('cleanword, 'count) -> 'results) }
        .map('results -> ()) { results: Seq[(String, Int)] =>
          val outMap = results.toMap

          // Validate that the output is as expected.
          assert(3 == outMap("hello"))
          assert(1 == outMap("world"))
        }
        // Write the result to a file.
        .write(NullSource)
        .getJob(args)
  }
}
