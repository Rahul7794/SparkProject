package com.rchallenge.assignments.transformation

import com.rchallenge.assignments.configurations.Config
import com.rchallenge.assignments.models.Customers
import org.apache.spark.sql.{DataFrame, Dataset}

object CohortComputation {
  def cohortAnalysisComputation(inputDf: Dataset[Customers], config: Config): DataFrame = {
    inputDf.toDF()
  }
}
