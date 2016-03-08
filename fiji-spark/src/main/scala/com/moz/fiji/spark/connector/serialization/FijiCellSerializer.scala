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
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.avro.Schema

import com.moz.fiji.schema.DecodedCell
import com.moz.fiji.schema.FijiCell
import com.moz.fiji.schema.FijiColumnName

/**
 * Kryo serializer for [[com.moz.fiji.schema.FijiCell]].
 */
class FijiCellSerializer[T] extends Serializer[FijiCell[T]] {
  override def write(
                      kryo: Kryo,
                      output: Output,
                      fijiCell: FijiCell[T]): Unit = {
    kryo.writeClassAndObject(output, fijiCell.getColumn.getFamily)
    kryo.writeClassAndObject(output, fijiCell.getColumn.getQualifier)
    kryo.writeClassAndObject(output, fijiCell.getTimestamp)
    kryo.writeClassAndObject(output, fijiCell.getData)
    kryo.writeClassAndObject(output, fijiCell.getReaderSchema)
    kryo.writeClassAndObject(output, fijiCell.getWriterSchema)
  }

  override def read(
                     kryo: Kryo,
                     input: Input,
                     clazz: Class[FijiCell[T]]): FijiCell[T] = {
    val family: String = kryo.readClassAndObject(input).asInstanceOf[String]
    val qualifier: String = kryo.readClassAndObject(input).asInstanceOf[String]
    val timeStamp: Long = kryo.readClassAndObject(input).asInstanceOf[Long]
    val data: T = kryo.readClassAndObject(input).asInstanceOf[T]
    val readerSchema: Schema = kryo.readClassAndObject(input).asInstanceOf[Schema]
    val writerSchema: Schema = kryo.readClassAndObject(input).asInstanceOf[Schema]
    FijiCell.create[T](FijiColumnName.create(family, qualifier), timeStamp,
      new DecodedCell[T](writerSchema, readerSchema, data))

  }
}