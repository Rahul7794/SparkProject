name := "truecaller"

organization := "com.truecaller.assignment"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"
)