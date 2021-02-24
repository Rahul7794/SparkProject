package com.rchallenge.assignments.transformation


import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.models.{Customers, GenericColumnsConstants}
import com.rchallenge.assignments.transformation.AverageComputation.{dailyDfComputation, hourlyDfComputation, monthlyDfComputation, weeklyDfComputation}
import com.rchallenge.assignments.transformation.CohortComputation.cohortAnalysisComputation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}

class TransformationComputation(implicit sparkSession: SparkSession) extends TransformerService[Customers] with GenericColumnsConstants {
  def parse: (String => Seq[(String, String)]) = {
    s =>
      val parsedDate = LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      Seq(
        ("hourly", s.substring(0, 13)+":00:00"),
        ("daily", parsedDate.toString),
        ("weekly", parsedDate.`with`(DayOfWeek.MONDAY).toString),
        ("monthly", parsedDate.withDayOfMonth(1).toString))
  }

  override def computeAverages(computationType: String, input_df: DataFrame): DataFrame = {

    computationType match {
      case "hourly" =>
        hourlyDfComputation(input_df)
      case "daily" =>
        dailyDfComputation(input_df)
      case "weekly" =>
        weeklyDfComputation(input_df)
      case "monthly" =>
        monthlyDfComputation(input_df)
    }
  }


  override def computeCohorts(config: Config)(df: Dataset[Customers]): DataFrame = {
    cohortAnalysisComputation(df, config)
  }

  override def groupDF(df: Dataset[Customers]): DataFrame = {
    import sparkSession.implicits._
    val myUDF: UserDefinedFunction = udf(parse)
    df
      .withColumn("tuple", explode(myUDF(date_format(col(CreatedAt), "yyyy-MM-dd hh:mm:ss"))))
      .select($"number", $"tuple._1".as("aggType"), $"tuple._2".as("aggValue"))
      .groupBy("number", "aggValue")
      .agg(count($"number") as "count", first($"aggType") as "aggType")
      .sort(col("aggValue"))

  }
}

object TransformationComputation {
  def apply(implicit sparkSession: SparkSession) = new TransformationComputation()
}