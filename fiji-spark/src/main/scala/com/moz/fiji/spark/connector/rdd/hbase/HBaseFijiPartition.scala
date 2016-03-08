package com.moz.fiji.spark.connector.rdd.hbase

import org.apache.spark.Partition

import com.moz.fiji.schema.EntityId
import com.moz.fiji.schema.HBaseEntityId

/**
 * A partition of Fiji data created by [[FijiRDD.getPartitions]].
 * Contains information needed by [[FijiRDD.compute]].
 *
 * @param mIndex the index of this partition.
 * @param mStartRow the start row of this partition.
 * @param mStopRow the stop row of this partition.
 */
class HBaseFijiPartition private (
    val mIndex: Int,
    val mStartRow: Array[Byte],
    val mStopRow: Array[Byte]
) extends Partition {

  /* Gets the row at which the partition starts, e.g. for a scanner. */
  def startLocation: EntityId = {
    HBaseEntityId.fromHBaseRowKey(mStartRow)
  }

  /* Gets the row at which the partition ends. */
  def stopLocation: EntityId = {
    HBaseEntityId.fromHBaseRowKey(mStopRow)
  }

  override def index: Int = mIndex
}

object HBaseFijiPartition {

  /**
   *
   * @param mIndex
   * @param mStartRow
   * @param mStopRow
   * @return
   */
  def apply(
    mIndex: Int,
    mStartRow: Array[Byte],
    mStopRow: Array[Byte]
  ): HBaseFijiPartition = {
    new HBaseFijiPartition(mIndex, mStartRow, mStopRow)
  }
}