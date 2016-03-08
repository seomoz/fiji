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

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.config.Config
import com.twitter.scalding.serialization.KryoHadoop
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.hbase.client.Result

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.impl.hbase.HBaseFijiRowData
import com.moz.fiji.schema.layout.ColumnReaderSpec

/**
 * Kryo specification that adds avro schema, generic record, and specific record serialization
 * support. Used with [[com.moz.fiji.express.flow.FijiJob]].
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
class FijiKryoInstantiator(config: Config) extends KryoHadoop(config) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()

    kryo.addDefaultSerializer(classOf[Schema], classOf[AvroSchemaSerializer])

    // Note: The order in which these two serializers are added matters. We want SpecificRecords to
    //     be picked up first before the more generic GenericContainer serializer. SpecificRecord is
    //     a subclass of GenericContainer.
    kryo.addDefaultSerializer(classOf[SpecificRecord], classOf[AvroSpecificSerializer])
    kryo.addDefaultSerializer(classOf[GenericContainer], classOf[AvroGenericSerializer])

    kryo.addDefaultSerializer(classOf[ColumnReaderSpec], classOf[ColumnReaderSpecSerializer])
    kryo.addDefaultSerializer(classOf[HBaseFijiRowData], classOf[HBaseFijiRowDataSerializer])
    kryo.addDefaultSerializer(classOf[FijiDataRequest], classOf[FijiDataRequestSerializer])
    kryo.addDefaultSerializer(classOf[FijiURI], classOf[FijiURISerializer])
    kryo.addDefaultSerializer(classOf[Result], classOf[ResultSerializer])


    kryo
  }
}
