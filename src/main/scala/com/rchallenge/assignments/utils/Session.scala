package com.rchallenge.assignments.utils

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
