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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.moz.fiji.spark.connector.serialization

import scala.collection.JavaConverters.asScalaIteratorConverter

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

import com.moz.fiji.schema.EntityId
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.impl.MaterializedFijiResult

/**
 * Kryo serializer for [[com.moz.fiji.schema.impl.MaterializedFijiResult]].
 * Non-implementation specific.
 */
class MaterializedFijiResultSerializer[T] extends Serializer[MaterializedFijiResult[T]] {
  override def write(
                      kryo: Kryo,
                      output: Output,
                      fijiResult: MaterializedFijiResult[T]): Unit = {
    kryo.writeClassAndObject(output, fijiResult.getDataRequest)
    kryo.writeClassAndObject(output, fijiResult.getEntityId)
    kryo.writeClassAndObject(output, fijiResult.iterator().asScala.toList)
  }

  override def read(
                     kryo: Kryo,
                     input: Input,
                     clazz: Class[MaterializedFijiResult[T]]): MaterializedFijiResult[T] = {
    val dataRequest: FijiDataRequest = kryo.readClassAndObject(input).asInstanceOf[FijiDataRequest]
    val entityId: EntityId = kryo.readClassAndObject(input).asInstanceOf[EntityId]
    val materials: List[FijiCell[T]] = kryo.readClassAndObject(input).asInstanceOf[List[FijiCell[T]]]

    // Uses Java collections because MaterializedFijiResult takes a Java SortedMap as a parameter
    val map = new java.util.TreeMap[FijiColumnName, java.util.List[FijiCell[T]]]
    for (cell: FijiCell[T] <- materials) {
      if (!map.containsKey(cell.getColumn)) {
        val list = new java.util.ArrayList[FijiCell[T]]()
        list.add(cell)
        map.put(cell.getColumn, list)
      } else {
        map.get(cell.getColumn).add(cell)
      }
    }
    val materializedResult =  map.asInstanceOf[java.util.SortedMap[FijiColumnName, java.util.List[FijiCell[T]]]]

    MaterializedFijiResult.create(entityId, dataRequest, materializedResult)
  }
}