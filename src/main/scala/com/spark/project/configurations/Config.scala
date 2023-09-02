package com.spark.project.configurations

import com.typesafe.config.ConfigFactory

case class Config(
                   Master: String,
                   AppName: String,
                   InputCSVPath: String,
                   OutputCSVPath: String,
                   InputJSONPath: String,
                   OutputJSONPath: String,
                   LogLevel: String,
                   BootStrapServer: String,
                   InputTopic: String,
                   UserEventTable: String,
                   InputMode: String,
                   OutputMode: String
                 )

object Config {
  def apply(): Config = {
    val config = ConfigFactory.load()
    val logLevel: String = config.getString("app.log_level")
    val inputCSVPath: String = config.getString("app.input.csv")
    val outputCSVPath: String = config.getString("app.output.csv")
    val inputJSONPath: String = config.getString("app.input.json")
    val outputJSONPath: String = config.getString("app.output.json")
    val master: String = config.getString("app.spark.master")
    val appName: String = config.getString("app.spark.app_name")
    val bootstrap: String = config.getString("app.input.kafka.bootstrap")
    val topic: String = config.getString("app.input.kafka.topic")
    val inputTable: String = config.getString("app.input.hive.tableName")
    val inputMode: String = config.getString("app.input.mode")
    val outputMode: String = config.getString("app.output.mode")
    Config(master, appName, inputCSVPath, outputCSVPath, inputJSONPath, outputJSONPath,
      logLevel, bootstrap, topic, inputTable, inputMode, outputMode)
  }
}