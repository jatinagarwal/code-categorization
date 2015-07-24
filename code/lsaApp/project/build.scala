import sbt._
import sbt.Keys._

object lsaAppBuild extends Build {

  lazy val root = Project(
    id = "lsaApp",
    base = file("."),
    settings = lsaAppSettings ++ Seq(libraryDependencies ++= Dependencies.lsaApp)
  )

  val scalacOptionsList = Seq("-encoding", "UTF-8", "-unchecked", "-optimize", "-deprecation", "-feature")

  // This is required for plugin devlopment.

  def lsaAppSettings =
    Defaults.coreDefaultSettings ++ Seq (
      name := "lsaApp",
      organization := "com.lsaApp",
      scalaVersion := "2.10.4",
      scalacOptions := scalacOptionsList,
      updateOptions := updateOptions.value.withCachedResolution(true),
      updateOptions := updateOptions.value.withLatestSnapshots(false),
      crossPaths := false,
      fork := true,
      javacOptions ++= Seq("-source", "1.7"),
      javaOptions += "-Xmx2048m",
      javaOptions += "-XX:+HeapDumpOnOutOfMemoryError"
    )

}

object Dependencies {
  val spark = "org.apache.spark" %% "spark-core" % "1.4.0" % "provided" //Provided makes it not run through sbt run.
  val sparkMlib = "org.apache.spark" %% "spark-mllib" % "1.4.0" % "provided"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test" 
  val slf4j = "org.slf4j" % "slf4j-log4j12" % "1.7.2"
  val javaparser = "com.github.javaparser" % "javaparser-core" % "2.0.0"
  val commonsIO = "commons-io" % "commons-io" % "2.4"

  val lsaApp = Seq(spark, sparkMlib, scalaTest, slf4j, javaparser, commonsIO)

  // transitively uses
  // commons-compress-1.4.1

}
