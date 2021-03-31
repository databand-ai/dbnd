val databandVersion = "0.37.1"
resolvers += "Databand Maven Repository" at "https://dbnd-dev-maven-repository.s3.us-east-2.amazonaws.com"

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := databandVersion
ThisBuild / organization := "ai.databand"
ThisBuild / organizationName := "databand"

lazy val root = (project in file("."))
    .settings(
        name := "spark_deequ_sbt",
        libraryDependencies ++= Seq(
            "ai.databand" % "dbnd-api-deequ" % databandVersion,
            "org.slf4j" % "slf4j-api" % "1.7.16",
            "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided"
        ),
        mainClass in Compile := Some("ai.databand.examples.Showcase")
    )

assemblyJarName in assembly := "spark_deequ_sbt-assembly-latest.jar"
