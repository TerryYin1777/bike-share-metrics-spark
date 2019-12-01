package com.terry.bigdata.io

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.terry.bigdata.config.BikeShareConfig
import com.terry.bigdata.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

trait RetentionIO extends Logging with Utils{

  def prepareForRetention(avgDurationDf: DataFrame, spark: SparkSession, conf: BikeShareConfig): Unit = {
    val retentionDir = conf.retentionPath()
    val outputPath = generatePath(retentionDir, conf.startDatePrefix(), conf.startDate())

    logInfo(s"Writing prepared retention file to ${outputPath}")

    val dfWithRetention = avgDurationDf
      .withColumn("day_1_retention", lit(-1))
      .withColumn("day_3_retention", lit(-1))
      .withColumn("day_7_retention", lit(-1))

    dfWithRetention.distinct().coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }

  def emptyRetentionDf(spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
      .withColumn("user_id", lit(null: StringType))
      .withColumn("subscriber_type", lit(null: StringType))
      .withColumn("start_station_id", lit(null: StringType))
      .withColumn("end_station_id", lit(null: StringType))
      .withColumn("zip_code", lit(null: StringType))
      .withColumn("duration_sec", lit(null: DoubleType))
  }

  def readRetention(spark: SparkSession, conf: BikeShareConfig): DataFrame = {
    val retentionPath = makeRetentionReadPath(conf)

    logInfo(s"Reading retention file from ${retentionPath}")

    val retentionDf = try {
      Some(spark.read.json(retentionPath)).getOrElse(emptyRetentionDf(spark))
    } catch {
      case e: Exception => emptyRetentionDf(spark)
    }
    retentionDf
  }

  def writeRetention(retentionDf: DataFrame, spark: SparkSession, conf: BikeShareConfig): Unit = {
    val retentionPath = makeRetentionWritePath(conf)
    retentionDf.distinct().coalesce(1).write.mode(SaveMode.Overwrite).json(retentionPath)
  }

  def makeRetentionReadPath(conf: BikeShareConfig): String = {
    val dayAgoDateString = getDayAgoDateString(conf)
    val inputPath = conf.dayAgo() match {
      case 1 => conf.retentionPath() + conf.startDatePrefix() + dayAgoDateString
      case 3 => conf.retentionPath() + "1/" + conf.startDatePrefix() + dayAgoDateString
      case 7 => conf.retentionPath() + "3/" + conf.startDatePrefix() + dayAgoDateString
    }
    inputPath
  }

  def makeRetentionWritePath(conf: BikeShareConfig): String = {
    val dayAgoDateString = getDayAgoDateString(conf)
    val outputPath = conf.dayAgo() match {
      case 1 => conf.retentionPath() + "1/" + conf.startDatePrefix() + dayAgoDateString
      case 3 => conf.retentionPath() + "3/" + conf.startDatePrefix() + dayAgoDateString
      case 7 => conf.retentionPath() + "7/" + conf.startDatePrefix() + dayAgoDateString
    }
    outputPath
  }

  def getDayAgoDateString(conf: BikeShareConfig): String = {
    val currentDateString = conf.startDate()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val parsedCurrentDate = LocalDate.parse(currentDateString, formatter)
    val parsedDayAgoDate = parsedCurrentDate.minusDays(conf.dayAgo())
    parsedDayAgoDate.toString
  }


}
