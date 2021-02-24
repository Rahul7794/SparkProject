package com.rchallenge.assignments.jobs

import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.reading.SourceReader
import com.rchallenge.assignments.transformation.TransformationComputation
import com.rchallenge.assignments.utils.Session
import com.rchallenge.assignments.writing.DestinationWriter
import org.apache.spark.sql.SparkSession

object Averages extends App {
  val config = Config()
  implicit val spark: SparkSession = Session.createSparkSession(config.Master, config.AppName)
  val reader = SourceReader(config)
  val writer = DestinationWriter(config)
  val compute = TransformationComputation(spark)

  val inputDF = reader.readFromCSVToDF()
  val groupedDF = compute.groupDF(inputDF)
  groupedDF.persist()
  val averageComputedDFHourly = compute.computeAverages("hourly", groupedDF)
  writer.printDFConsole(averageComputedDFHourly)
  val averageComputedDFDaily = compute.computeAverages("daily", groupedDF)
  writer.printDFConsole(averageComputedDFDaily)
  val averageComputedDFWeekly = compute.computeAverages("weekly", groupedDF)
  writer.printDFConsole(averageComputedDFWeekly)
  val averageComputedDFMonthly = compute.computeAverages("monthly", groupedDF)
  writer.printDFConsole(averageComputedDFMonthly)
  System.in.read()
}
