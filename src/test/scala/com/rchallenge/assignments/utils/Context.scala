package com.rchallenge.assignments.utils

import com.rchallenge.assignments.configurations.Config
import org.apache.spark.sql.SparkSession

trait Context {
  val config: Config = Config()
  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master(config.Master)
      .appName(config.AppName)
      .getOrCreate()
}
