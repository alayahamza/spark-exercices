package org.halaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SalariesGap {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("SalariesGap")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val data = spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/salaries.csv")
      .cache()

    data.printSchema()
    data.show(100, truncate = false)

    val windowSpecAgg = Window.partitionBy(col("department")).orderBy(col("department").desc)
    val result = data
      .withColumn("max", max(col("salary")).over(windowSpecAgg))
      .withColumn("diff", col("max") - col("salary"))

    result.printSchema()
    result.show(100, truncate = false)

  }
}
