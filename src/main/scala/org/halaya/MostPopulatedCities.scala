package org.halaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, regexp_replace, upper}
import org.apache.spark.sql.types.IntegerType

object MostPopulatedCities {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("MostPopulatedCities")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val cities = Seq(
      ("Warsaw", "Poland", "1 764 615"),
      ("Cracow", "Poland", "769 498"),
      ("Paris", "France", "2 206 488"),
      ("Villeneuve-Loubet", "France", "15 020"),
      ("Chicago IL", "United States", "2 716 000"),
      ("Milwaukee WI", "United States", "595 351"),
      ("Pittsburgh PA", "United States", "302 407"),
      ("Vilnius", "Lithuania", "580 020"),
      ("Stockholm", "Sweden", "972 647"),
      ("Goteborg", "Sweden", "580 020")).toDF("name", "country", "population")
    cities.show()

    cities
      .withColumn("populationInt", regexp_replace(col("population"), " ", "").cast(IntegerType))
      .withColumn("rank", max(col("populationInt")).over(Window.partitionBy("country").orderBy(col("populationInt").desc)))
      .filter(col("populationInt") === col("rank"))
      .orderBy(col("populationInt").desc)
      .withColumn("country", upper(col("country")))
      .withColumn("name", upper(col("name")))
      .drop("rank", "populationInt")
      .show(truncate = false)
  }
}
