package org.halaya

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}

object ExplodingStructsArray {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("ExplodingStructsArray")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val input = spark.read
      .option("multiline", "true")
      .json("src/main/resources/input.json")
      .cache()

    val index = input.schema.fieldIndex("hours")
    val propSchema = input.schema(index).dataType.asInstanceOf[StructType]
    val columns = collection.mutable.LinkedHashSet[Column]()
    propSchema.fields.foreach(field => {
      columns.add(lit(field.name))
      columns.add(col("hours." + field.name))
    })

    val explodedInput = input
      .withColumn("hours", map(columns.toSeq: _*))
      .withColumn("hours", map(columns.toSeq: _*))
      .select(
        col("business_id"),
        col("full_address"),
        explode(col("hours")).as(Seq("day","hours"))
      )
      .withColumn("open_time", col("hours.open"))
      .withColumn("close_time", col("hours.close"))
      .drop("hours")

    explodedInput.printSchema()
    explodedInput.show(100, truncate = false)
  }
}
