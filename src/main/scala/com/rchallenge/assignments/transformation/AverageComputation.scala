package com.rchallenge.assignments.transformation

import com.rchallenge.assignments.models.GenericColumnsConstants
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object AverageComputation extends GenericColumnsConstants {


  def hourlyDfComputation(input_df: DataFrame): DataFrame = {
    input_df
      .filter(col("aggType").equalTo("hourly"))
      .withColumn("hourly_average", when(col("aggType").equalTo("hourly"), col("count") / 60))
      .select("number", "aggValue", "count", "hourly_average")
  }

  def dailyDfComputation(input_df: DataFrame): DataFrame = {

    input_df
      .filter(col("aggType").equalTo("daily"))
      .withColumn("daily_average",when(col("aggType").equalTo("daily"), col("count") / 24))
      .select("number", "aggValue", "count", "daily_average")
  }

  def weeklyDfComputation(input_df: DataFrame): DataFrame = {
    input_df
      .filter(col("aggType").equalTo("weekly"))
      .withColumn("weekly_average", when(col("aggType").equalTo("weekly"), col("count")/ 7))
      .select("number", "aggValue", "count", "weekly_average")
  }

  def monthlyDfComputation(input_df: DataFrame): DataFrame = {
    input_df
      .filter(col("aggType").equalTo("monthly"))
      .withColumn("monthly_average",when(col("aggType").equalTo("monthly") ,col("count") / dayofmonth(last_day(col("aggValue")))))
      .select("number", "aggValue", "count", "monthly_average")
  }
}
