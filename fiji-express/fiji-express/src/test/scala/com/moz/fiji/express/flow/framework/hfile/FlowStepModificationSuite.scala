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

import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Mode
import junit.framework.Assert
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.flow.FijiJob
import com.moz.fiji.express.flow.FijiOutput
import com.moz.fiji.express.flow.util.TestingResourceUtil
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.util.InstanceBuilder

/**
 * Tests that the modifications we perform on the FlowDef as part of FijiJobs with HFile outputs
 * are functionally correct.
 */
@RunWith(classOf[JUnitRunner])
class FlowStepModificationSuite extends FijiClientTest with FijiSuite {
  // Hook into FijiClientTest since methods marked with JUnit's @Before and @After annotations won't
  // run when using ScalaTest.
  setupFijiTest()

  val avroLayout: FijiTableLayout = TestingResourceUtil.layout("layout/avro-types-complete.json")
  val uri = {
    val instanceUri = new InstanceBuilder(getFiji)
        .withTable(avroLayout.getName, avroLayout)
        .build()
        .getURI

    val tableUri = FijiURI.newBuilder(instanceUri).withTableName(avroLayout.getName).build()
    tableUri.toString
  }
  val args = Mode.putMode(Hdfs(strict = false, getConf), Args(Nil))

  test("A map-only hfile output job is compiled to one stage.") {
    val flow = new MapOnlyHFile(uri, args).buildFlow
    Assert.assertEquals(1, flow.getFlowSteps.size)
  }

  test("A map + reduce hfile output job is compiled to two stages.") {
    val flow = new MapReduceHFile(uri, args).buildFlow
    Assert.assertEquals(2, flow.getFlowSteps.size)
  }

  test("A map-only direct output job is compiled to one stage.") {
    val flow = new MapOnlyDirect(uri, args).buildFlow
    Assert.assertEquals(1, flow.getFlowSteps.size)
  }

  test("A map + reduce direct output job is compiled to one stage.") {
    val flow = new MapReduceDirect(uri, args).buildFlow
    Assert.assertEquals(1, flow.getFlowSteps.size)
  }

  test("A map + reduce and map only hfile output job is compiled to three stages.") {
    val flow = new MapOnlyAndMapReduceHFile(uri, args).buildFlow
    Assert.assertEquals(3, flow.getFlowSteps.size)
  }

  test("A map + reduce and map only direct output job is compiled to two stages.") {
    val flow = new MapOnlyAndMapReduceDirect(uri, args).buildFlow
    Assert.assertEquals(2, flow.getFlowSteps.size)
  }
}

class MapOnlyHFile(uri: String, args: Args) extends FijiJob(args) {
  IterableSource(List("x"), ('entityId, 'x))
    .read
    .write(HFileFijiOutput.builder
        .withTableURI(uri)
        .withHFileOutput("/")
        .withColumns('x -> "strict:string")
        .build)
}

class MapReduceHFile(uri: String, args: Args) extends FijiJob(args) {
  IterableSource(List("x"), ('entityId, 'x))
    .read
    .groupAll( x => x.size )
    .insert('entityId, "fuzz")
    .write(HFileFijiOutput.builder
      .withTableURI(uri)
      .withHFileOutput("/")
      .withColumns('size -> "strict:long")
      .build)
}

class MapOnlyDirect(uri: String, args: Args) extends FijiJob(args) {
  IterableSource(List("x"), ('entityId, 'x))
    .read
    .write(FijiOutput.builder.withTableURI(uri).withColumns('x -> "strict:string").build)
}

class MapReduceDirect(uri: String, args: Args) extends FijiJob(args) {
  IterableSource(List("x"), ('entityId, 'x))
    .read
    .groupAll( x => x.size )
    .insert('entityId, "fuzz")
    .write(FijiOutput.builder.withTableURI(uri).withColumns('size -> "strict:long").build)
}

class MapOnlyAndMapReduceHFile(uri: String, args: Args) extends FijiJob(args) {
  val pipe = IterableSource(List("x"), ('entityId, 'x)).read

  pipe.write(HFileFijiOutput.builder
      .withTableURI(uri)
      .withHFileOutput("/")
      .withColumns('x -> "strict:string")
      .build)

  pipe
    .groupAll( x => x.size )
    .insert('entityId, "fuzz")
    .write(HFileFijiOutput.builder
      .withTableURI(uri)
      .withHFileOutput("/")
      .withColumns('size -> "strict:long")
      .build)
}

class MapOnlyAndMapReduceDirect(uri: String, args: Args) extends FijiJob(args) {
  val pipe = IterableSource(List("x"), ('entityId, 'x)).read

  pipe.write(FijiOutput.builder.withTableURI(uri).withColumns('x -> "strict:string").build)

  pipe
    .groupAll( x => x.size )
    .insert('entityId, "fuzz")
    .write(FijiOutput.builder.withTableURI(uri).withColumns('size -> "strict:long").build)
}
