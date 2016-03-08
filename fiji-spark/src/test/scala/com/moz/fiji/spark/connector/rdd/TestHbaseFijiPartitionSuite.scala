// (c) Copyright 2014 WibiData, Inc.
package com.moz.fiji.spark.connector.rdd

import org.junit.Assert
import org.junit.Test
import com.moz.fiji.schema.EntityId
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.avro.ComponentType
import com.moz.fiji.schema.avro.HashSpec
import com.moz.fiji.schema.avro.RowKeyComponent
import com.moz.fiji.schema.avro.RowKeyEncoding
import com.moz.fiji.schema.avro.RowKeyFormat2
import com.moz.fiji.spark.connector.rdd.hbase.HBaseFijiPartition

import scala.collection.JavaConverters.seqAsJavaListConverter

class TestHbaseFijiPartitionSuite {
  import com.moz.fiji.spark.connector.rdd.TestHbaseFijiPartitionSuite._

  @Test
  def simpleFijiPartition() {
    val partition: HBaseFijiPartition = HBaseFijiPartition(
      INDEX,
      START_ENTITYID.getHBaseRowKey,
      STOP_ENTITYID.getHBaseRowKey
    )
    Assert.assertEquals(START_ENTITYID.getHBaseRowKey, partition.startLocation.getHBaseRowKey)
    Assert.assertEquals(STOP_ENTITYID.getHBaseRowKey, partition.stopLocation.getHBaseRowKey)
    Assert.assertEquals(INDEX, partition.index)
  }
}

object TestHbaseFijiPartitionSuite {
  val COMPONENTS = List(
      RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
      RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
      RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build()
  )
  val FORMAT: RowKeyFormat2 = RowKeyFormat2.newBuilder()
      .setEncoding(RowKeyEncoding.FORMATTED)
      .setSalt(HashSpec.newBuilder().build())
      .setComponents(COMPONENTS.asJava)
      .build()

  val ENTITYID_FACTORY = EntityIdFactory.getFactory(FORMAT)
  val START_ENTITYID: EntityId = ENTITYID_FACTORY
      .getEntityId(
          "start": java.lang.String,
          0: java.lang.Integer,
          0L: java.lang.Long
      )
  val STOP_ENTITYID: EntityId = ENTITYID_FACTORY
      .getEntityId(
          "stop": java.lang.String,
          1: java.lang.Integer,
          1L: java.lang.Long
      )

  val INDEX = 1
}
