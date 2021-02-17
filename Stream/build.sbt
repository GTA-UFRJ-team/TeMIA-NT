ThisBuild / organization := "br.ufrj.gta"

scalaVersion := "2.11.6"

val sparkVersion: String = "2.4.0"

lazy val stream = (project in file("."))
    .settings(
        name := "Stream",
        libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
	libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.0" % "provided"
    )
