package com.terry.bigdata.process

import com.terry.bigdata.config.BikeShareConfig
import com.terry.bigdata.io.{DurationIO, RetentionIO, TripsIO}
import com.terry.bigdata.utils.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DurationMetric extends TripsIO with Utils with DurationIO with RetentionIO{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Duration")
      .getOrCreate()

    val conf = new BikeShareConfig(args)
    val groupFields = loadColumns(conf, "duration.group.columns")
    calcDuration(spark, conf, groupFields)
  }

  def calcDuration(spark: SparkSession, conf: BikeShareConfig, fields: List[String]): Unit = {
    val dailyTrips = readTrips(spark, conf)
    val avgDuration = dailyTrips
      .groupBy(fields.map(col):_*)
      .agg(avg("duration_sec").as("avg_duration_sec"))

    writeDuration(avgDuration, spark, conf)
    prepareForRetention(avgDuration, spark, conf)
  }
}
