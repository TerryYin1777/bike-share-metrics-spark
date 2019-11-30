package com.terry.bigdata.config

import org.rogach.scallop.{ScallopConf,ScallopOption}

class BikeShareConfig(args: Seq[String]) extends ScallopConf(args) with Serializable {

  val validEnv = List("test", "prod")

  val env: ScallopOption[String] = opt[String](
    name = "env",
    descr = "environment flag, should be either test or prod",
    required = true,
    default = Option("prod"),
    validate = validEnv.contains(_)
  )

  val tripsPath: ScallopOption[String] = opt[String](
    name = "trips.path",
    descr = "path to the daily trips",
    required = false,
    default = env() match {
      case "test" => Option("gs://bike-share-data/test/bike_trips/")
      case "prod" => Option("gs://bike-share-data/bike_trips/")
    }
  )

  val uniqueUserPath: ScallopOption[String] = opt[String](
    name = "unique.user.path",
    descr = "path to the unique user list",
    required = false,
    default = env() match {
      case "test" => Option("gs://bike-share-data/test/bike/unique-user/")
      case "prod" => Option("gs://bike-share-data/bike/unique-user/")
    }
  )

  val uniqueUserTempPath: ScallopOption[String] = opt[String](
    name = "unique.user.temp",
    descr = "path to the temp unique user list",
    required = false,
    default = env() match {
      case "test" => Option("gs://bike-share-data/test/bike/temp/")
      case "prod" => Option("gs://bike-share-data/bike/temp/")
    }
  )

  val startDate: ScallopOption[String] = opt[String](
    name = "start.date",
    descr = "the date from which the application start to process, in the form of yyyy-mm-dd",
    required = true
  )

  val startDatePrefix: ScallopOption[String] = opt[String](
    name = "prefix",
    descr = "start date prefix",
    required = false,
    default = Option("start_date")
  )

  val columnSelectionConfFile: ScallopOption[String] = opt[String](
    name = "conf.columns.selection",
    descr = "Name of the file containing columns selection configurations",
    required = false,
    default = Option("columns-selection")
  )

  val columnSelectionTripsKey: ScallopOption[String] = opt[String](
    name = "trip.columns.key",
    required = false,
    default = Option("trips.columns")
  )

  val columnSelectionUniqueUsersKey: ScallopOption[String] = opt[String](
    name = "user.columns.key",
    required = false,
    default = Option("unique.users.columns")
  )

  verify()
}
