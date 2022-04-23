package org.halaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object SplitFunctionWithVariableDelimiterPerRow{

  val spark: SparkSession = SparkSession.builder()
  .master("local[1]")
  .appName("Application")
  .getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val dept = Seq(
      ("50000.0#0#0#", "#"),
      ("0@1000.0@", "@"),
      ("1$", "$"),
      ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
    dept.show()

    dept.withColumn("split_values", expr("""split(values, concat("\\", delimiter))"""))
      .show(truncate = false)
  }
}
