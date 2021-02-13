package com.rchallenge.assignments.transformation


import com.rchallenge.assignments.models.{Customers, GenericColumnsConstants}
import com.rchallenge.assignments.transformation.AverageComputation.{DailyDfComputation, HourlyDfComputation, MonthlyDfComputation, WeeklyDfComputation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

class AverageComputation extends TransformerService[Customers] with GenericColumnsConstants {
  override def computeAverages(computationType: String)(input_df: Dataset[Customers]): DataFrame = {

    computationType match {
      case "hourly" =>
        HourlyDfComputation(input_df)
      case "daily" =>
        DailyDfComputation(input_df)
      case "weekly" =>
        WeeklyDfComputation(input_df)
      case "monthly" =>
        MonthlyDfComputation(input_df)
    }
  }
}

object AverageComputation extends GenericColumnsConstants {
  def apply() = new AverageComputation()

  def HourlyDfComputation(input_df: Dataset[Customers]): DataFrame = {
    val groupDfByHour =
      input_df
        .withColumn("hourly", date_trunc("hour", col(CreatedAt)))
        .groupBy("number", "hourly").count()
    groupDfByHour
      .withColumn("hourly", col("hourly"))
      .withColumn("hourly_average", col("count") / 60)
      .sort(col("hourly"))
  }

  def DailyDfComputation(input_df: Dataset[Customers]): DataFrame = {
    val groupDfByDay =
      input_df
        .withColumn("daily", date_trunc("day", col(CreatedAt)))
        .groupBy("number", "daily").count()

    groupDfByDay
      .withColumn("daily", to_date(col("daily")))
      .withColumn("daily_average", col("count") / 24)
      .sort(col("daily"))
  }

  def WeeklyDfComputation(input_df: Dataset[Customers]): DataFrame = {
    val groupDfByWeek =
      input_df
        .withColumn("week", date_trunc("week", col(CreatedAt)))
        .groupBy("number", "week").count()

    groupDfByWeek
      .withColumn("week", to_date(col("week")))
      .withColumn("weekly_average", col("count") / 7)
      .sort(col("week"))
  }

  def MonthlyDfComputation(input_df: Dataset[Customers]): DataFrame = {
    val groupDFByMonth =
      input_df
        .withColumn("month", date_trunc("month", col(CreatedAt)))
        .groupBy("number", "month").count()

    groupDFByMonth
      .withColumn("month", date_format(to_date(col("month")), "yyyy-MM"))
      .withColumn("monthly_average", col("count") / dayofmonth(last_day(col("month"))))
      .sort(col("month"))
  }
}
