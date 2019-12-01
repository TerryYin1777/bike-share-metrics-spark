package com.terry.bigdata.process

import com.terry.bigdata.config.BikeShareConfig
import com.terry.bigdata.io.{DurationIO, RetentionIO, TripsIO, UniqueUserIO}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RetentionMetrics extends UniqueUserIO with TripsIO with RetentionIO with DurationIO {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Retention")
      .getOrCreate()

    val conf = new BikeShareConfig(args)

    calcRetention(spark, conf)
  }

  def calcRetention(spark: SparkSession, conf: BikeShareConfig): Unit = {
    val dailyTrips = readTrips(spark, conf)
    val uniqueUsers = readUniqueUsers(spark, conf)
    val retention = readRetention(spark, conf)

    val tripsWithFirstStamp = dailyTrips.join(uniqueUsers,
      dailyTrips("user_id") === uniqueUsers("user_id"),
      "left").drop(uniqueUsers("user_id"))

    import spark.implicits._

    val tripsWithUserAge = tripsWithFirstStamp
      .withColumn("user_age_days", datediff($"start_timestamp", $"first_timestamp"))

    val userAgeFiltered = tripsWithUserAge
      .select($"user_id", $"user_age_days")
      .filter($"user_age_days" === conf.dayAgo())
      .distinct()

    val retentionWithUserAge = retention.join(userAgeFiltered,
      retention("user_id") === userAgeFiltered("user_id"),
      "left").drop(userAgeFiltered("user_id"))

    val retentionColumnName = s"day_${conf.dayAgo()}_retention"

    val retentionUpdated = retentionWithUserAge
      .withColumn(retentionColumnName, when($"user_age_days".isNotNull, 1).otherwise(0))
      .drop("user_age_days")

    writeRetention(retentionUpdated, spark, conf)

  }

}
