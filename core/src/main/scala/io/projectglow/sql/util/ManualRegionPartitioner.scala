package io.projectglow.sql.util

import org.apache.spark.Partitioner

class ManualRegionPartitioner[V](partitions: Int) extends Partitioner {

  override def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    key match {
      case (_, f2: Int) => f2
      case i: Int => i
      case _ =>
        throw new Exception(
          "Unable to partition key %s without destination assignment.".format(key)
        )
    }
  }
}
