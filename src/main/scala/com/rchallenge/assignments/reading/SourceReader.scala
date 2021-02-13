package com.rchallenge.assignments.reading

import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.models.Customers
import org.apache.spark.sql.{Dataset, SparkSession}


class SourceReader(config: Config)(implicit spark: SparkSession) extends ReaderService[Customers] {
  override def readFromCSVToDF(): Dataset[Customers] = {
    import spark.implicits._
    spark
      .read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(config.InputPath)
      .as[Customers]
  }
}

object SourceReader {
  def apply(config: Config)(implicit spark: SparkSession) = new SourceReader(config)
}
