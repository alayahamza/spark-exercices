package org.halaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, min}

object SelectingTheMostImportantRowsPerAssignedPriority {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Application")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val input = Seq(
      (1, "MV1"),
      (1, "MV2"),
      (2, "VPV"),
      (2, "Others")).toDF("id", "value")

    input.orderBy(col("value").desc).filter(col("id") === input.agg(min("id")))
      .show(truncate = false)
  }
}
