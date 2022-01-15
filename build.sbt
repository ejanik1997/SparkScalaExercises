name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.spark" %% "spark-avro" % "2.4.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"

)