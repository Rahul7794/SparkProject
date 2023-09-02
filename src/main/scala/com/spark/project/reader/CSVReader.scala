package com.spark.project.reader

import com.spark.project.configurations.Config
import com.spark.project.models.UserEvent
import org.apache.spark.sql.{Dataset, SparkSession}

class CSVReader(config: Config)(implicit spark: SparkSession) extends ReaderService[UserEvent] {
  override def loadDataToDF(): Dataset[UserEvent] = {
    import spark.implicits._
    spark
      .read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(config.InputCSVPath)
      .as[UserEvent]
  }

}

object CSVReader {
  def apply(config: Config)(implicit spark: SparkSession) = new CSVReader(config)
}
