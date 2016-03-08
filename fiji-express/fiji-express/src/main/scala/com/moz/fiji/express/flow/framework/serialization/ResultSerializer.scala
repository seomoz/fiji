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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos

/**
 * Kryo serializer for [[org.apache.hadoop.hbase.client.Result]].  Takes advantage of the fact
 * that Result implements Serializable, and uses java DataOutputStream/DataInputStream to
 * write/read.
 */
class ResultSerializer extends Serializer[Result] {
  override def write(kryo: Kryo, output: Output, result: Result): Unit = {
    val protoResult: ClientProtos.Result = ProtobufUtil.toResult(result)
    protoResult.writeTo(output)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[Result]): Result = {
    val protoResult: ClientProtos.Result =
      ClientProtos.Result.parseFrom(input)
    val result: Result = ProtobufUtil.toResult(protoResult)
    result
  }
}
