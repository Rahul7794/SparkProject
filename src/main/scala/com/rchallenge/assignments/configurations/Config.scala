package com.rchallenge.assignments.configurations

import com.typesafe.config.ConfigFactory

case class Config(
                   Master: String,
                   AppName: String,
                   InputPath: String,
                   OutputPath: String,
                   LogLevel: String,
                   CohortWeeksDate: String,
                   CohortNoOfWeeks: Int
                 )

object Config {
  def apply(): Config = {
    val config = ConfigFactory.load()
    val logLevel: String = config.getString("app.log_level")
    val inputPath: String = config.getString("app.spark.input_file_path")
    val outputPath: String = config.getString("app.spark.output_file_path")
    val master: String = config.getString("app.spark.master")
    val appName: String = config.getString("app.spark.app_name")
    val weekDate: String = config.getString("app.spark.cohort.week_date")
    val noOfWeeks: Int = config.getInt("app.spark.cohort.no_of_weeks")
    Config(master, appName, inputPath, outputPath, logLevel, weekDate, noOfWeeks)
  }
}
