package com.rchallenge.assignments.jobs

import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.reading.SourceReader
import com.rchallenge.assignments.transformation.AverageComputation
import com.rchallenge.assignments.utils.Session
import com.rchallenge.assignments.writing.DestinationWriter
import org.apache.spark.sql.SparkSession

object Averages extends App {
  val config = Config()
  implicit val spark: SparkSession = Session.createSparkSession(config.Master, config.AppName)
  val reader = SourceReader(config)
  val writer = DestinationWriter(config)
  val compute = AverageComputation()

  val averageComputedDFHourly =
    reader.readFromCSVToDF()
      .transform(compute.computeAverages("hourly"))
  writer.printDFConsole(averageComputedDFHourly)

  val averageComputedDFDaily =
    reader.readFromCSVToDF()
      .transform(compute.computeAverages("daily"))
  writer.printDFConsole(averageComputedDFDaily)

  val averageComputedDFWeekly =
    reader.readFromCSVToDF()
      .transform(compute.computeAverages("weekly"))
  writer.printDFConsole(averageComputedDFWeekly)

  val averageComputedDFMonthly =
    reader.readFromCSVToDF()
      .transform(compute.computeAverages("monthly"))
  writer.printDFConsole(averageComputedDFMonthly)
}
