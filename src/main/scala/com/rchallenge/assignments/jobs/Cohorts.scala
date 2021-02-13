package com.rchallenge.assignments.jobs

import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.reading.SourceReader
import com.rchallenge.assignments.transformation.AverageComputation
import com.rchallenge.assignments.utils.Session
import com.rchallenge.assignments.writing.DestinationWriter
import org.apache.spark.sql.SparkSession

//object Cohorts extends App {
//  val config = Config()
//  val reader = SourceReader(config)
//  val writer = DestinationWriter(config)
//  val compute = AverageComputation()
//
//  implicit val spark: SparkSession = Session.createSparkSession(config.Master, config.AppName)
//
//
//}
