package org.halaya

import com.sun.deploy.util.SessionState.save
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.halaya.Stream.writeBatch

import java.util.Properties

object Stream {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Application")
      .getOrCreate()


    val df = spark.
      readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "backblaze_smart")
      //      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()

    val stringDF = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("date", StringType)
      .add("serial_number", StringType)
      .add("model", StringType)
      .add("capacity_bytes", IntegerType)

    val dataFrame = stringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")


      .writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      println("batchId : " + batchId)
      batchDF.printSchema()
      batchDF.show(100, truncate = false)

      val mode = "append"
      val url = "jdbc:postgresql://localhost:5432/postgres"
      val properties: Properties = new Properties()
      properties.setProperty("user", "postgres_user")
      properties.setProperty("password", "postgres_password")
      properties.setProperty("driver", "org.postgresql.Driver")
      batchDF.write.mode(mode).jdbc(url = url, table = "BACKBLAZE_SMART", properties)
    }.start()
      .awaitTermination()

    //    dataFrame.writeStream
    //      .format("json")
    //      .option("path", "src/main/resources/stream.json")
    //      .option("checkpointLocation", "src/main/resources/tmp")
    //      .outputMode(OutputMode.Append())
    //      .start()
    //      .awaitTermination()
  }

  private def writeBatch = {
    (batchDF: Dataset[Row]) =>
      batchDF.persist()
      batchDF.write.format("json").save("src/main/resources/stream") // location 1
      batchDF.unpersist()
  }
}
