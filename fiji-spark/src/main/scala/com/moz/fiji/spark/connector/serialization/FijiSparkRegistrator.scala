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

import com.esotericsoftware.kryo.Kryo
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.serializer.KryoRegistrator

import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.impl.MaterializedFijiResult

/**
 * Registers serializer classes for a SparkJob
 * @tparam T type of MaterializedFijiResult
 */
class FijiSparkRegistrator[T] extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.addDefaultSerializer(classOf[SpecificRecord], classOf[AvroSpecificSerializer])
    kryo.addDefaultSerializer(classOf[GenericRecord], classOf[AvroGenericSerializer])
    kryo.addDefaultSerializer(classOf[Schema], classOf[AvroSchemaSerializer])
    kryo.register(classOf[MaterializedFijiResult[T]], new MaterializedFijiResultSerializer[T]())
    kryo.register(classOf[FijiDataRequest], new FijiDataRequestSerializer())
    kryo.register(classOf[FijiCell[T]], new FijiCellSerializer())
  }
}