package com.spark.project.utils

import org.apache.spark.sql.SparkSession

object Session {
  def createSparkSession(master: String, appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }
}
  