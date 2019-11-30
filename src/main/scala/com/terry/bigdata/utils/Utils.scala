package com.terry.bigdata.utils

import com.terry.bigdata.config.BikeShareConfig
import com.typesafe.config.ConfigFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._


trait Utils extends Logging{

  def generatePath(dir: String, conf: BikeShareConfig): String = {
    dir + conf.startDatePrefix() + "=" + conf.startDate()
  }

  def dataFrameSelectColumns(df: DataFrame, key: String, conf: BikeShareConfig): DataFrame = {
    val columns = loadColumns(conf, key).map(col)
    df.select(columns:_*)
  }

  def loadColumns(conf: BikeShareConfig, columnsKey: String): List[String] = {
    val columnsString: List[String] = try {
      ConfigFactory.load(conf.columnSelectionConfFile()).getStringList(columnsKey).toList
    } catch {
      case e: Exception => logError(s"Error retrieving columns for the key ${columnsKey}")
        List[String]()
    }
    columnsString
  }

}
