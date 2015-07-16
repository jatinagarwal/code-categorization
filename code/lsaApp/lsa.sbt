name := "Lsa Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10" % "1.4.0",
  "org.apache.spark"  % "spark-mllib_2.10" % "1.4.0",
  "commons-io" % "commons-io" % "2.4",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  )

