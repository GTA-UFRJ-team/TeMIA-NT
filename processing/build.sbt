ThisBuild / organization := "br.ufrj.gta"

scalaVersion := "2.12.12"

val sparkVersion: String = "3.0.1"

lazy val stream = (project in file("."))
    .settings(
        name := "Stream",
        libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    )
