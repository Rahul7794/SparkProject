package com.rchallenge.assignments.reading

import org.apache.spark.sql.Dataset

trait ReaderService[T] {
  def readFromCSVToDF(): Dataset[T]
}
