package com.terry.bigdata.io

import com.terry.bigdata.config.BikeShareConfig
import com.terry.bigdata.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

trait TripsIO extends Logging with Utils{

  def emptyTripsDf(spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
      .withColumn("user_id", lit(null: StringType))
      .withColumn("subscriber_type", lit(null: StringType))
      .withColumn("start_station_id", lit(null: StringType))
      .withColumn("end_station_id", lit(null: StringType))
      .withColumn("zip_code", lit(null: StringType))
      .withColumn("start_timestamp", lit(null: StringType))
      .withColumn("duration_sec", lit(null: DoubleType))
  }

  def readTrips(spark: SparkSession, conf: BikeShareConfig): DataFrame = {
    val tripsDir = conf.tripsPath()
    val tripsPath = generatePath(tripsDir, conf.startDatePrefix(), conf.startDate())

    logInfo(s"Loading trips from ${tripsPath}")

    val tripsDf = try {
      Some(spark.read.json(tripsPath)).getOrElse(emptyTripsDf(spark))
    } catch {
      case e: Exception => emptyTripsDf(spark)
    }
    dataFrameSelectColumns(tripsDf, conf.columnSelectionTripsKey(), conf)
  }

}
