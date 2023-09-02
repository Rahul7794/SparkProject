package com.spark.project.writer

import com.spark.project.configurations.Config
import com.spark.project.models.Output
import org.apache.spark.sql.Dataset

class ConsoleWriter(config: Config) extends WriterService[Output] {
  override def writeData(outDF: Dataset[Output]): Unit = {
    outDF.show(false)
  }
}

object ConsoleWriter {
  def apply(config: Config) = new ConsoleWriter(config)
}
