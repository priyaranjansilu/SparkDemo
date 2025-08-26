package com.silu.now

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {


    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Hudi Example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()



    import spark.implicits._

    // Define table properties
    val tableName = "hudi_trips"
    val basePath = "C:\\Users\\priya\\OneDrive\\Desktop\\hudaiAll"
    val recordKey = "trip_id"
    val partitionField = "region"
    val precombineField = "timestamp"

    // Create a sample DataFrame
    val data = Seq(
      ("1", "2024-02-01", "NY", 100),
      ("2", "2024-02-02", "CA", 200),
      ("3", "2024-02-03", "TX", 300)
    ).toDF("trip_id", "timestamp", "region", "fare")

    // Write data to Hudi
    val hudiOptions = Map(
      "hoodie.table.name" -> tableName,
      RECORDKEY_FIELD.key() -> recordKey,
      PRECOMBINE_FIELD.key() -> precombineField,
      PARTITIONPATH_FIELD.key() -> partitionField,
      TABLE_TYPE.key() -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "insert"
    )

    data.write.format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Read from Hudi
    val hudiDF = spark.read.format("hudi").load(basePath)
    hudiDF.show()
    println("hello")
    // Update data
    val updatedData = Seq(
      ("1", "2024-02-05", "NY", 150), // Update trip_id = 1
      ("4", "2024-02-06", "FL", 400) // New trip_id = 4
    ).toDF("trip_id", "timestamp", "region", "fare")

    val updateOptions = hudiOptions + ("hoodie.datasource.write.operation" -> "upsert")

    updatedData.write.format("hudi")
      .options(updateOptions)
      .mode(SaveMode.Append)
      .save(basePath)

    // Read updated data
    val updatedDF = spark.read.format("hudi").load(basePath)
    updatedDF.show()

    // Stop Spark Session
    spark.stop()
  }
}
