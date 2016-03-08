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
package com.moz.fiji.spark.connector.serialization

import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.util.{List => JList}

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.avro.util.Utf8
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.Ignore

import com.moz.fiji.schema.EntityId
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiDataRequestBuilder
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef
import com.moz.fiji.schema.FijiResult
import com.moz.fiji.schema.impl.MaterializedFijiResult
import com.moz.fiji.schema.impl.hbase.HBaseFijiTable
import com.moz.fiji.schema.impl.hbase.HBaseFijiTableReader
import com.moz.fiji.schema.layout.FijiTableLayouts
import com.moz.fiji.schema.util.InstanceBuilder
import com.moz.fiji.spark.connector.serialization.FijiSparkRegistrator

class TestKryoSerializerSuite extends FijiClientTest {
  import TestKryoSerializerSuite._

  @Ignore("Until testKryoSerializer is reenabled")
  @Before
  def setupTestHBaseFijiResult() {
    writeData(getFiji)
    mTable = HBaseFijiTable.downcast(getFiji.openTable("all_types_table"))
    mReader = mTable.openTableReader.asInstanceOf[HBaseFijiTableReader]
  }

  @Ignore("Until testKryoSerializer is reenabled")
  @After
  def cleanupTestHBaseFijiRowView() {
    mTable.release
    mReader.close
    //created to test the kryo serializer
    Files.delete(Paths.get("file.bin"))
  }

  @Ignore("Broken, the fix is pending an overhaul to FijiSpark testing framework")
  @Test
  def testKryoSerializer[T]() = {
    val instance: ScalaKryoInstantiator = new ScalaKryoInstantiator()
    val kryo = instance.newKryo()

    new FijiSparkRegistrator[T].registerClasses(kryo)

    val builder: FijiDataRequestBuilder = FijiDataRequest
      .builder
      .addColumns(ColumnsDef
      .create
      .withMaxVersions(10)
      .add(PRIMITIVE_STRING, null)
      .add(STRING_MAP_1, null))

    val request: FijiDataRequest = builder.build
    val eid: EntityId = mTable.getEntityId(ROW)
    val view: FijiResult[T] = mReader.getResult(eid, request)
    val map: java.util.SortedMap[FijiColumnName, JList[FijiCell[T]]] = FijiResult
      .Helpers
      .getMaterializedContents[T](view)
    val matResult: MaterializedFijiResult[T] =
      MaterializedFijiResult.create(view.getEntityId, view.getDataRequest, map)
    val output = new Output(new FileOutputStream("file.bin"))
    kryo.writeClassAndObject(output, matResult);
    output.close();
    val input = new Input(new FileInputStream("file.bin"))
    val newMatResult: MaterializedFijiResult[T] = kryo
      .readClassAndObject(input)
      .asInstanceOf[MaterializedFijiResult[T]]
    input.close()

    //test that the contents are the same
    val firstIterator = matResult.iterator
    val secondIterator = newMatResult.iterator
    while(firstIterator.hasNext) {
      Assert.assertTrue(newMatResult.iterator.hasNext)
      Assert.assertEquals(firstIterator.next, secondIterator.next)
    }

  }
  private var mTable: HBaseFijiTable = null
  private var mReader: HBaseFijiTableReader = null
}

object TestKryoSerializerSuite {
  private final val PRIMITIVE_FAMILY: String = "primitive"
  private final val STRING_MAP_FAMILY: String = "string_map"
  private final val PRIMITIVE_STRING: FijiColumnName = FijiColumnName
      .create(PRIMITIVE_FAMILY, "string_column")
  private final val STRING_MAP_1: FijiColumnName = FijiColumnName
      .create(STRING_MAP_FAMILY, "smap_1")
  private final val ROW: Integer = 1

  def writeData(fiji: Fiji): Unit = {
    new InstanceBuilder(fiji)
        .withTable(FijiTableLayouts.getLayout("com.moz.fiji/schema/layout/all-types-schema.json"))
        .withRow(ROW)
        .withFamily(PRIMITIVE_FAMILY)
        .withQualifier("string_column")
        .withValue(10L, new Utf8("ten"))
        .withValue(5L, new Utf8("five"))
        .withValue(4L, new Utf8("four"))
        .withValue(3L, new Utf8("three"))
        .withValue(2L, new Utf8("two"))
        .withValue(1L, new Utf8("one"))
        .withFamily(STRING_MAP_FAMILY)
        .withQualifier("smap_1")
        .withValue(10L, new Utf8("sm1-ten"))
        .withValue(5L, new Utf8("sm1-five"))
        .withValue(4L, new Utf8("sm1-four"))
        .withValue(3L, new Utf8("sm1-three"))
        .withValue(2L, new Utf8("sm1-two"))
        .withValue(1L, new Utf8("sm1-one"))
        .build

  }
}
