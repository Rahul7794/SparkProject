package com.rchallenge.assignments.transformation

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.rchallenge.assignments.models.Customers
import com.rchallenge.assignments.utils.Context
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, _}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AverageComputationTest extends AnyFlatSpec with Matchers with Context with BeforeAndAfter with DataFrameComparer {

//  trait Fixture {
//    implicit val sparkSession: SparkSession = spark
//  }
//
//  behavior of "AverageComputations"
//
//  it should "return weekly averages" in new Fixture {
//
//    import spark.implicits._
//
//    val customerDS: Dataset[Customers] = Seq(
//      ("2018-04-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-15 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-16 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-22 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-22 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-05-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-05-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002)
//    ).toDF("ts", "number", "pick_lat", "pick_lng", "drop_lat", "drop_lng").as[Customers]
//
//    val compute: TransformationComputation = TransformationComputation(spark)
//    val actualDF: Dataset[Row] = customerDS.transform(compute.computeAverages("weekly")).toDF()
//
//    val rawDF: DataFrame = Seq(
//      ("14626", "2018-04-02", 2, 0.2857142857142857),
//      ("14626", "2018-04-09", 1, 0.14285714285714285),
//      ("14626", "2018-04-16", 3, 0.42857142857142855),
//      ("14626", "2018-05-07", 2, 0.2857142857142857)
//    ).toDF("number", "week", "count", "weekly_average")
//
//    val schemaDF: DataFrame = Seq(("number", "string"), ("week", "date"), ("count", "long"), ("weekly_average", "Double")).toDF("columnName", "columnType")
//
//    val dataTypes: DataFrame = schemaDF.select("columnName", "columnType")
//    val listOfElements: Array[List[Any]] = dataTypes.collect.map(_.toSeq.toList)
//    val validationTemplate: (Any, Any) => Column = (c: Any, t: Any) => {
//      val column = c.asInstanceOf[String]
//      val typ = t.asInstanceOf[String]
//      col(column).cast(typ)
//    }
//    val expectedDF: DataFrame = rawDF.select(listOfElements.map(element => validationTemplate(element.head, element(1))): _*)
//
//    assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
//  }
//
//  it should "return monthly averages" in new Fixture {
//
//    import spark.implicits._
//
//    val customerDS: Dataset[Customers] = Seq(
//      ("2018-04-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 07:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-05-15 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-05-16 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-05-22 07:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-06-22 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-06-07 07:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-06-07 07:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002)
//    ).toDF("ts", "number", "pick_lat", "pick_lng", "drop_lat", "drop_lng").as[Customers]
//
//    val compute: TransformationComputation = TransformationComputation(spark)
//    val actualDF: Dataset[Row] = customerDS.transform(compute.computeAverages("monthly")).toDF()
//    val rawDF: DataFrame = Seq(
//      ("14626", "2018-04", 1, 0.03333333333333333),
//      ("14627", "2018-04", 1, 0.03333333333333333),
//      ("14626", "2018-05", 2, 0.06451612903225806),
//      ("14627", "2018-05", 1, 0.03225806451612903),
//      ("14627", "2018-06", 2, 0.06666666666666667),
//      ("14626", "2018-06", 1, 0.03333333333333333)
//    ).toDF("number", "month", "count", "monthly_average")
//
//    val schemaDF: DataFrame =
//      Seq(("number", "string"), ("month", "string"), ("count", "long"), ("monthly_average", "Double"))
//        .toDF("columnName", "columnType")
//
//    val dataTypes: DataFrame = schemaDF.select("columnName", "columnType")
//    val listOfElements: Array[List[Any]] = dataTypes.collect.map(_.toSeq.toList)
//    val validationTemplate: (Any, Any) => Column = (c: Any, t: Any) => {
//      val column = c.asInstanceOf[String]
//      val typ = t.asInstanceOf[String]
//      col(column).cast(typ)
//    }
//    val expectedDF: DataFrame = rawDF.select(listOfElements.map(element => validationTemplate(element.head, element(1))): _*)
//
//    assertLargeDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
//  }
//
//  it should "return daily averages" in new Fixture {
//
//    import spark.implicits._
//
//    val customerDS: Dataset[Customers] = Seq(
//      ("2018-04-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 08:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 09:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 10:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 11:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 12:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 13:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 14:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002)
//    ).toDF("ts", "number", "pick_lat", "pick_lng", "drop_lat", "drop_lng").as[Customers]
//
//    val compute: TransformationComputation = TransformationComputation(spark)
//    val actualDF: Dataset[Row] = customerDS.transform(compute.computeAverages("daily")).toDF()
//    val rawDF: DataFrame = Seq(
//      ("14626", "2018-04-07", 4, 0.16666666666666666),
//      ("14627", "2018-04-07", 4, 0.16666666666666666)
//    ).toDF("number", "daily", "count", "daily_average")
//
//    val schemaDF: DataFrame =
//      Seq(("number", "string"), ("daily", "date"), ("count", "long"), ("daily_average", "Double"))
//        .toDF("columnName", "columnType")
//
//    val dataTypes: DataFrame = schemaDF.select("columnName", "columnType")
//    val listOfElements: Array[List[Any]] = dataTypes.collect.map(_.toSeq.toList)
//    val validationTemplate: (Any, Any) => Column = (c: Any, t: Any) => {
//      val column = c.asInstanceOf[String]
//      val typ = t.asInstanceOf[String]
//      col(column).cast(typ)
//    }
//    val expectedDF: DataFrame = rawDF.select(listOfElements.map(element => validationTemplate(element.head, element(1))): _*)
//
//    assertLargeDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
//  }
//
//  it should "return hourly averages" in new Fixture {
//
//    import spark.implicits._
//
//    val customerDS: Dataset[Customers] = Seq(
//      ("2018-04-07 07:07:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 07:08:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 07:09:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 07:10:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 08:07:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 08:08:17", "14626", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 08:09:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002),
//      ("2018-04-07 08:10:17", "14627", 12.3136215, 76.65819499999998, 12.287301, 76.60228000000002)
//    ).toDF("ts", "number", "pick_lat", "pick_lng", "drop_lat", "drop_lng").as[Customers]
//
//    val compute: TransformationComputation = TransformationComputation(spark)
//    val actualDF: Dataset[Row] = customerDS.transform(compute.computeAverages("hourly")).toDF()
//    val rawDF: DataFrame = Seq(
//      ("14626", "2018-04-07 07:00:00", 3, 0.05),
//      ("14627", "2018-04-07 07:00:00", 1, 0.016666666666666666),
//      ("14626", "2018-04-07 08:00:00", 1, 0.016666666666666666),
//      ("14627", "2018-04-07 08:00:00", 3, 0.05)
//    ).toDF("number", "hourly", "count", "hourly_average")
//
//    val schemaDF: DataFrame =
//      Seq(("number", "string"), ("hourly", "timestamp"), ("count", "long"), ("hourly_average", "double"))
//        .toDF("columnName", "columnType")
//
//    val dataTypes: DataFrame = schemaDF.select("columnName", "columnType")
//    val listOfElements: Array[List[Any]] = dataTypes.collect.map(_.toSeq.toList)
//    val validationTemplate: (Any, Any) => Column = (c: Any, t: Any) => {
//      val column = c.asInstanceOf[String]
//      val typ = t.asInstanceOf[String]
//      col(column).cast(typ)
//    }
//    val expectedDF: DataFrame = rawDF.select(listOfElements.map(element => validationTemplate(element.head, element(1))): _*)
//
//    assertLargeDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
//  }

}
