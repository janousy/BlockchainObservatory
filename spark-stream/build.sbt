ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-spark-ingest",

    libraryDependencies += "org.apache.spark" %% "spark-core" % SPARK_VERSION,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
    // https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % SPARK_VERSION,
    libraryDependencies += "io.delta" %% "delta-core" % "1.2.0"
  )

logLevel := Level.Debug
val SPARK_VERSION = "3.2.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


