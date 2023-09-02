package com.spark.project.writer

import com.spark.project.configurations.Config
import com.spark.project.models.Output
import org.apache.spark.sql.Dataset

class CSVWriter(config: Config) extends WriterService[Output] {
  override def writeData(outDF: Dataset[Output]): Unit = {
    outDF
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(config.OutputCSVPath)
  }
}

object CSVWriter {
  def apply(config: Config) = new CSVWriter(config)
}
