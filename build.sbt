name := "myo-streaming"

version := "1.0"

scalaVersion := "2.10.5"

val sparkCore = "org.apache.spark" % "spark-core_2.10" % "1.4.0"
val sparkSql = "org.apache.spark" % "spark-sql_2.10" % "1.4.0"
val sparkMlLib = "org.apache.spark" % "spark-mllib_2.10" % "1.4.0"
val sparkStreaming = "org.apache.spark" % "spark-streaming_2.10" % "1.4.0"

val sparkCsv = "com.databricks" % "spark-csv_2.10" % "1.1.0"

libraryDependencies ++= Seq(
  sparkCore,
  sparkSql,
  sparkMlLib,
  sparkStreaming,
  sparkCsv
)