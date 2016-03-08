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

package com.moz.fiji.express.flow.framework.serialization

import scala.collection.JavaConverters.seqAsJavaListConverter

import cascading.kryo.KryoFactory
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.SerDeSuite
import com.moz.fiji.express.avro.SimpleRecord
import cascading.tuple.collect.SpillableProps
import cascading.pipe.assembly.AggregateBy
import com.twitter.chill.config.{ConfiguredInstantiator, ScalaMapConfig}

@RunWith(classOf[JUnitRunner])
class KryoFijiSuite
    extends FunSuite
    with SerDeSuite {
  def kryoDeepCopy[T](kryo: Kryo, data: T): T = {
    val output = new Output(1024)
    kryo.writeObject(output, data)

    val klazz = data.getClass
    val input = new Input(output.getBuffer)
    kryo.readObject(input, klazz)
  }

  val recordSchema: Schema = {
    val fields = Seq(
        new Schema.Field("field1", Schema.create(Schema.Type.INT), "First test field.", null),
        new Schema.Field("field2", Schema.create(Schema.Type.STRING), "First test field.", null),
        new Schema.Field("field3", Schema.create(Schema.Type.FLOAT), "First test field.", null))

    val record = Schema.createRecord("TestRecord", "", "", false)
    record.setFields(fields.asJava)

    record
  }

  val genericRecord: GenericContainer = {
    new GenericRecordBuilder(recordSchema)
        .set("field1", 42)
        .set("field2", "foo")
        .set("field3", 3.14f)
        .build()
  }

  val specificRecord: SpecificRecord = {
    SimpleRecord
        .newBuilder()
        .setL(42L)
        .setO("foo")
        .setS("bar")
        .build()
  }

  serDeTest("Schema", "Kryo", recordSchema) { actual =>
    // Use cascading.kryo to mimic scalding's actual behavior.

    val kryo = new Kryo()
    val kryoFactory = new KryoFactory(HBaseConfiguration.create())
    val registrations = Seq(
        new KryoFactory.ClassPair(classOf[Schema], classOf[AvroSchemaSerializer]))
    kryoFactory.setHierarchyRegistrations(registrations.asJava)
    kryoFactory.populateKryo(kryo)

    // Serialize and deserialize the schema.
    kryoDeepCopy(kryo, actual)
  }

  serDeTest("GenericRecord", "Kryo", genericRecord) { actual =>
    // Use cascading.kryo to mimic scalding's actual behavior.
    val kryo = new Kryo()
    val kryoFactory = new KryoFactory(HBaseConfiguration.create())
    val registrations = Seq(
        new KryoFactory.ClassPair(classOf[GenericContainer], classOf[AvroGenericSerializer]))
    kryoFactory.setHierarchyRegistrations(registrations.asJava)
    kryoFactory.populateKryo(kryo)

    // Serialize and deserialize the GenericRecord.
    kryoDeepCopy(kryo, actual)
  }

  serDeTest("SpecificRecord", "Kryo", specificRecord) { actual =>
    // Use cascading.kryo to mimic scalding's actual behavior.
    val kryo = new Kryo()
    val kryoFactory = new KryoFactory(HBaseConfiguration.create())
    val registrations = Seq(
        new KryoFactory.ClassPair(classOf[SpecificRecord], classOf[AvroSpecificSerializer]))
    kryoFactory.setHierarchyRegistrations(registrations.asJava)
    kryoFactory.populateKryo(kryo)

    // Serialize and deserialize the SpecificRecord.
    kryoDeepCopy(kryo, actual)
  }

  def kryoFijiTest[T](inputName: String, input: => T) {
    serDeTest(inputName, "KryoFiji", input) { actual =>
      // Setup a Kryo object using KryoFiji.
      val defaultSpillThreshold = 100 * 1000
      val lowPriorityDefaults = Map(
          SpillableProps.LIST_THRESHOLD -> defaultSpillThreshold.toString,
          SpillableProps.MAP_THRESHOLD -> defaultSpillThreshold.toString,
          AggregateBy.AGGREGATE_BY_THRESHOLD -> defaultSpillThreshold.toString
      )
      val chillConf = ScalaMapConfig(lowPriorityDefaults)

      assert(
          // Contains is implemented backwards in chill 0.3.6.
          chillConf.contains(ConfiguredInstantiator.KEY),
          s"Found ${ConfiguredInstantiator.KEY} in ${chillConf.toMap}"
      )
      ConfiguredInstantiator.setReflect(chillConf, classOf[FijiKryoInstantiator])
      assert(
          // Contains is implemented backwards in chill 0.3.6.
          !chillConf.contains(ConfiguredInstantiator.KEY),
          s"Could not find ${ConfiguredInstantiator.KEY} in ${chillConf.toMap}"
      )
      assert(
          chillConf.get(ConfiguredInstantiator.KEY) == classOf[FijiKryoInstantiator].getName,
          s"Configured kryo instantiator is ${chillConf.get(ConfiguredInstantiator.KEY)}"
      )

      val kryo = new ConfiguredInstantiator(chillConf).newKryo()

      // Serialize and deserialize the input data.
      kryoDeepCopy(kryo, actual)
    }
  }

  kryoFijiTest("Schema", recordSchema)
  kryoFijiTest("GenericRecord", genericRecord)
  kryoFijiTest("SpecificRecord", specificRecord)
}
