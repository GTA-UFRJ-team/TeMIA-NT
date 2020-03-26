ThisBuild / organization := "br.ufrj.gta"

scalaVersion := "2.11.12"

val sparkVersion: String = "2.4.5"

lazy val stream = (project in file("."))
    .settings(
        name := "Stream",
        libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
        libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "7.6.1"
    )
