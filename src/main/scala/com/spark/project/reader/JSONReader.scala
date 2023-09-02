package com.spark.project.reader

import com.spark.project.configurations.Config
import com.spark.project.models.UserEvent
import org.apache.spark.sql.{Dataset, SparkSession}

class JSONReader(config: Config)(implicit spark: SparkSession) extends ReaderService[UserEvent] {
  override def loadDataToDF(): Dataset[UserEvent] = {
    import spark.implicits._
    spark
      .read
      .json(config.InputJSONPath)
      .as[UserEvent]
  }
}

object JSONReader {
  def apply(config: Config)(implicit spark: SparkSession) = new JSONReader(config)
}