package com.spark.project.reader

import com.spark.project.configurations.Config
import com.spark.project.models.UserEvent
import org.apache.spark.sql.{Dataset, SparkSession}

class HiveReader(config: Config)(implicit spark: SparkSession) extends ReaderService[UserEvent] {
  override def loadDataToDF(): Dataset[UserEvent] = {
    import spark.implicits._
    spark
      .table(config.UserEventTable).as[UserEvent]
  }
}

object HiveReader {
  def apply(config: Config)(implicit spark: SparkSession) = new HiveReader(config)
}
