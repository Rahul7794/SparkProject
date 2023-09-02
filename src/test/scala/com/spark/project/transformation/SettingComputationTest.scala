package com.spark.project.transformation

import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparer}
import com.spark.project.models.{Output, UserEvent}
import com.spark.project.utils.Context
import com.truecaller.assignment.models.Output
import org.apache.spark.sql.{SparkSession, _}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SettingComputationTest extends AnyFlatSpec with Matchers with Context with DatasetComparer {
  trait Fixture {
    implicit val sparkSession: SparkSession = spark
  }

  behavior of "Setting Computations"
  it should "return setting compute based on id" in new Fixture {

    import spark.implicits._

    val inputDS: Dataset[UserEvent] = spark.createDataset(Seq(
      UserEvent(1, "notification", "true", 1546333200),
      UserEvent(3, "refresh", "denied", 1546334200),
      UserEvent(2, "background", "notDetermined", 1546333611),
      UserEvent(3, "refresh", "4", 1546333443),
      UserEvent(1, "notification", "false", 1546335647),
      UserEvent(1, "background", "true", 1546333546)
    ))
    val transformer: TransformerService = SettingComputations(spark)

    val actualDS: Dataset[Output] = inputDS.transform(transformer.transform)

    val expectDS: Dataset[Output] = spark.createDataset(Seq(
      Output(1, Map("notification" -> "false", "background" -> "true")),
      Output(2, Map("background" -> "notDetermined")),
      Output(3, Map("refresh" -> "denied"))
    ))
    assertLargeDatasetEquality(actualDS, expectDS, ignoreNullable = true)
  }
}
