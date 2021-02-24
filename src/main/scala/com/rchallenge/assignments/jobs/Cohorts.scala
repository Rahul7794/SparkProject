package com.rchallenge.assignments.jobs

import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.reading.SourceReader
import com.rchallenge.assignments.transformation.{AverageComputation, TransformationComputation}
import com.rchallenge.assignments.utils.Session
import com.rchallenge.assignments.writing.DestinationWriter
import org.apache.spark.sql.SparkSession

object Cohorts extends App {
  val config = Config()
  implicit val spark: SparkSession = Session.createSparkSession(config.Master, config.AppName)
  val reader = SourceReader(config)
  val writer = DestinationWriter(config)
  val compute = TransformationComputation(spark)


  val cohortComputed =
    reader.readFromCSVToDF()
      .transform(compute.computeCohorts(config))

  writer.printDFConsole(cohortComputed)
}
