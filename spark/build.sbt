ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "graph_builder",

    libraryDependencies += "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
  )

logLevel := Level.Info
val SPARK_VERSION = "3.2.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
