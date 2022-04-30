package org.halaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, regexp_replace, upper}
import org.apache.spark.sql.types.IntegerType

object JsonGen {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("JsonGen")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val data = spark
      .read
      .option("multiline", "true")
      .json("src/main/resources/json")

    data.printSchema()
    data.select(
      col("index"),
      col("_id"),
      col("age"),
      col("gender")
    )
      .orderBy(col("_id").desc)
      .show(100, truncate = false)
  }
}
