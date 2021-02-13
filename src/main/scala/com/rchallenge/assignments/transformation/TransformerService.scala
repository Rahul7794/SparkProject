package com.rchallenge.assignments.transformation

import org.apache.spark.sql.{DataFrame, Dataset}

trait TransformerService[T] {
  def computeAverages(computationType: String)(df: Dataset[T]): DataFrame
}
