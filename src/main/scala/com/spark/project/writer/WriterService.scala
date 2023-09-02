package com.spark.project.writer

import com.spark.project.configurations.Config
import com.spark.project.models.Output
import org.apache.spark.sql.Dataset

trait WriterService[T] {
  def writeData(outDF: Dataset[T]): Unit
}

object WriterService {
  def apply(mode: String, config: Config): WriterService[Output] = {
    mode match {
      case "csv" => new CSVWriter(config)
      case "console" => new ConsoleWriter(config)
    }
  }
}
