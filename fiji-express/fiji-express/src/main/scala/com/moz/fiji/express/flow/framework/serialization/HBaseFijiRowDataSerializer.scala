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

package com.moz.fiji.express.flow.framework.serialization

import com.moz.fiji.schema.EntityId
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.impl.hbase.HBaseFijiTable
import com.moz.fiji.schema.impl.hbase.HBaseFijiRowData

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.hadoop.hbase.client.Result

/**
 * Kryo serializer for [[com.moz.fiji.schema.impl.hbase.HBaseFijiRowData]].
 */
class HBaseFijiRowDataSerializer extends Serializer[HBaseFijiRowData] {
  override def write(kryo: Kryo, output: Output, fijiRowData: HBaseFijiRowData): Unit = {
    // Write the FijiURI as a string.  Unfortunately using Kryo built-in serialization for it
    // leads to errors because it cannot modify an underlying immutable collection.
    kryo.writeClassAndObject(output, fijiRowData.getTable().getURI)
    kryo.writeClassAndObject(output, fijiRowData.getDataRequest())
    kryo.writeClassAndObject(output, fijiRowData.getEntityId())
    kryo.writeClassAndObject(output, fijiRowData.getHBaseResult())
    // Do not attempt to write the CellDecoderProvider.  It can get created again on the other
    // side.
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[HBaseFijiRowData]): HBaseFijiRowData = {
    val fijiURI: FijiURI = kryo.readClassAndObject(input).asInstanceOf[FijiURI]
    val dataRequest: FijiDataRequest = kryo.readClassAndObject(input).asInstanceOf[FijiDataRequest]
    val entityId: EntityId = kryo.readClassAndObject(input).asInstanceOf[EntityId]
    val result: Result = kryo.readClassAndObject(input).asInstanceOf[Result]
    val table: HBaseFijiTable =
      HBaseFijiTable.downcast(Fiji.Factory.get().open(fijiURI).openTable(fijiURI.getTable))

    // Initialize a new HBaseFijiRowData.  It will create a new CellDecoderProvider since we pass
    // in null.
    new HBaseFijiRowData(table, dataRequest, entityId, result, null)
  }
}
