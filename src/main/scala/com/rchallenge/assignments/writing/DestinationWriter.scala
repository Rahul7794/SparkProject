package com.rchallenge.assignments.writing

import com.rchallenge.assignments.configurations.Config
import org.apache.spark.sql.DataFrame

class DestinationWriter(config: Config) extends WriterService {
  override def writeDFToCSV(outputDF: DataFrame): Unit = {
    outputDF
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(config.OutputPath)
  }

  override def printDFConsole(outputDF: DataFrame): Unit = {
    outputDF.show(false)
  }
}

object DestinationWriter {
  def apply(config: Config) = new DestinationWriter(config)
}
