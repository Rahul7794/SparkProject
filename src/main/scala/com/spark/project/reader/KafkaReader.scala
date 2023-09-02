package com.spark.project.reader

import com.spark.project.configurations.Config
import com.spark.project.models.UserEvent
import org.apache.spark.sql.{Dataset, SparkSession}

class KafkaReader(config: Config)(implicit spark: SparkSession) extends ReaderService[UserEvent] {
  override def loadDataToDF(): Dataset[UserEvent] = {
    import spark.implicits._
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.BootStrapServer)
      .option("subscribe", config.InputTopic)
      .option("startingOffsets", "earliest")
      .load()
      .as[UserEvent]
  }

}

object KafkaReader {
  def apply(config: Config)(implicit spark: SparkSession) = new KafkaReader(config)
}
