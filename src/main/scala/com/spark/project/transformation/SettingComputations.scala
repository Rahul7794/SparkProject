package com.spark.project.transformation

import com.spark.project.models.{Output, UserEvent}
import com.truecaller.assignment.models.Output
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class SettingComputations(implicit spark: SparkSession) extends TransformerService {
  def transform(data: Dataset[UserEvent]): Dataset[Output] = {
    import spark.implicits._
    val df1 =
      data
      .orderBy(col("timestamp"))
      .groupBy(col("id"))
      .agg(collect_list(map(col("name"), col("value"))) as "settings")
      .orderBy(col("id"))

    val joinMap = udf { values: Seq[Map[String, String]] => values.flatten.toMap }
    df1.withColumn("settings", joinMap(col("settings"))).as[Output]
  }
}

object SettingComputations {
  def apply(implicit sparkSession: SparkSession) = new SettingComputations()
}
