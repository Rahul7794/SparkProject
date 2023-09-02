package com.spark.project.transformation

import com.spark.project.models.{Output, UserEvent}
import com.truecaller.assignment.models.Output
import org.apache.spark.sql.{Dataset, SparkSession}

trait TransformerService {
  def transform(data: Dataset[UserEvent]): Dataset[Output]
}

object TransformerService {
  def apply(process: String)(implicit spark: SparkSession): TransformerService = {
    process match {
      case "settings" => new SettingComputations()
    }
  }
}
