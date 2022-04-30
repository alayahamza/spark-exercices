package org.halaya

import com.sun.deploy.util.SessionState.save
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.halaya.Stream.writeBatch

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


    //    dataFrame.writeStream.foreachBatch((batchDF: Dataset[Row], batchId: Long) => {
    //      writeBatch(batchDF)
    //    })
    //  }

    dataFrame.writeStream
      .format("json")
      .option("path", "src/main/resources/stream.json")
      .option("checkpointLocation", "src/main/resources/tmp")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  private def writeBatch = {
    (batchDF: Dataset[Row]) =>
      batchDF.persist()
      batchDF.write.format("json").save("src/main/resources/stream") // location 1
      batchDF.unpersist()
  }
}
