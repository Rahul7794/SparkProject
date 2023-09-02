package com.spark.project.jobs

import com.spark.project.configurations.Config
import com.spark.project.models.{Output, UserEvent}
import com.spark.project.reader
import com.spark.project.reader.ReaderService
import com.spark.project.transformation.TransformerService
import com.spark.project.utils.Session
import com.spark.project.writer.WriterService
import com.truecaller.assignment.models.Output
import org.apache.spark.sql.{Dataset, SparkSession}

object DailyUser {
  def main(args: Array[String]): Unit = {
    val config = Config()
    implicit val spark: SparkSession = Session.createSparkSession(config.Master, config.AppName)
    val reader = reader.ReaderService(config.InputMode, config)
    val writer = WriterService(config.OutputMode, config)
    val transformer = TransformerService("settings")

    val inputDS: Dataset[UserEvent] = reader.loadDataToDF()
    val result: Dataset[Output] = inputDS.transform(transformer.transform)
    writer.writeData(result)
  }
}
