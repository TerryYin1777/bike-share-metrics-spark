package com.terry.bigdata.io

import com.terry.bigdata.config.BikeShareConfig
import com.terry.bigdata.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait DurationIO extends Logging with Utils{

  def writeDuration(avgDurationDf: DataFrame, spark: SparkSession, conf: BikeShareConfig): Unit = {
    val outputPath = generatePath(conf.avgDurationPath(), conf.startDatePrefix(), conf.startDate())

    logInfo(s"Writing average duration to ${outputPath}")
    avgDurationDf.distinct().coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }
}
