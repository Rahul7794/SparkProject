package com.rchallenge.assignments.transformation

import com.rchallenge.assignments.configurations.Config
import org.apache.spark.sql.{DataFrame, Dataset}

trait TransformerService[T] {
  def computeAverages(computationType: String,df: DataFrame): DataFrame
  def groupDF(df:Dataset[T]):DataFrame
  def computeCohorts(config: Config)(df: Dataset[T]): DataFrame
}
