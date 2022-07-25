val databandVersion = "0.83.1"

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := databandVersion
ThisBuild / organization := "ai.databand"
ThisBuild / organizationName := "databand"

lazy val root = (project in file("."))
    .settings(
        name := "spark_deequ_sbt",
        libraryDependencies ++= Seq(
            "ai.databand" % "dbnd-api-deequ" % databandVersion,
            "com.amazon.deequ" % "deequ" % "1.2.2-spark-2.4",
            "org.slf4j" % "slf4j-api" % "1.7.16",
            "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided"
        ),
        mainClass in Compile := Some("ai.databand.examples.Showcase")
    )

assemblyJarName in assembly := "spark_deequ_sbt-assembly-latest.jar"

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}
