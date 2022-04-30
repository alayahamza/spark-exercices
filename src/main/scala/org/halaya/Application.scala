package org.halaya

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import scala.::
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Application {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Application")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val dataFrame = generateDF(spark)
    dataFrame.printSchema()
    dataFrame.show(100, truncate = false)
  }

  def generateDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("6265570dedb1a687aa9a40a0", "MV1"),
      ("6265570dedb1a687aa9a40a0", "Others")
    )
      .toDF("id", "value")
  }
}
