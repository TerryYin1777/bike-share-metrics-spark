package com.terry.bigdata.io

import com.terry.bigdata.config.BikeShareConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

trait UniqueUserIO extends Logging {

  def emptyUniqueUsersDf(spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
      .withColumn("user_id", lit(null: StringType))
      .withColumn("first_timestamp", lit(null: StringType))
  }

  def readUniqueUsers(spark: SparkSession, conf: BikeShareConfig): DataFrame = {
    val inputPath = conf.uniqueUserPath()

    logInfo(s"Loading unique users from ${inputPath}")

    val uniqueUsersDf = try {
      Some(spark.read.json(inputPath)).getOrElse(emptyUniqueUsersDf(spark))
    } catch {
      case e:Exception => emptyUniqueUsersDf(spark)
    }
    uniqueUsersDf
  }

  def writeUniqueUsers(updatedDf: DataFrame, spark: SparkSession, conf: BikeShareConfig): Unit = {
    val outputTempPath = conf.uniqueUserTempPath()
    val outputPath = conf.uniqueUserPath()

    logInfo(s"Writing unique users to ${outputPath}")

    updatedDf.distinct().write.mode(SaveMode.Overwrite).json(outputTempPath)
    val temp = spark.read.json(outputTempPath)
    temp.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }



}
