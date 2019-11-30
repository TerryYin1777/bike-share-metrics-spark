package com.terry.bigdata.process

import com.terry.bigdata.config.BikeShareConfig
import com.terry.bigdata.io.{TripsIO, UniqueUserIO}
import com.terry.bigdata.utils.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UniqueUserUpdate extends UniqueUserIO with TripsIO with Utils{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UniqueUser")
      .getOrCreate()

    val conf = new BikeShareConfig(args)

    uniqueUserUpdate(spark, conf)
  }

  def uniqueUserUpdate(spark: SparkSession, conf: BikeShareConfig): Unit = {
    val uniqueUsers = readUniqueUsers(spark, conf)
    val dailyTrips = readTrips(spark, conf)

    val dailyTripsUsers = dataFrameSelectColumns(dailyTrips, conf.columnSelectionUniqueUsersKey(), conf)
      .withColumnRenamed("start_timestamp", "first_timestamp")

    val updatedUniqueUsers = uniqueUsers
      .unionByName(dailyTripsUsers)
      .groupBy("user_id")
      .agg(min("first_timestamp").as("first_timestamp"))

    writeUniqueUsers(updatedUniqueUsers, spark, conf)
  }

}
