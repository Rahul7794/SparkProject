package com.spark.project.reader

import com.spark.project.configurations.Config
import com.spark.project.models.UserEvent
import org.apache.spark.sql.{Dataset, SparkSession}


trait ReaderService[T] {
  def loadDataToDF(): Dataset[T]
}

object ReaderService {
  def apply(inputMode: String, config: Config)(implicit spark: SparkSession): ReaderService[UserEvent] = {
    inputMode match {
      case "csv" => new CSVReader(config)
      case "json" => new JSONReader(config)
      case "kafka" => new KafkaReader(config)
      case "hive" => new HiveReader(config)
    }
  }
}
