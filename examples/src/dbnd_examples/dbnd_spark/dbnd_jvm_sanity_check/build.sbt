val databandVersion = "0.29.6"
resolvers += "Databand Maven Repository" at "https://dbnd-dev-maven-repository.s3.us-east-2.amazonaws.com"

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := databandVersion
ThisBuild / organization := "ai.databand"
ThisBuild / organizationName := "databand"

lazy val root = (project in file("."))
    .settings(
        name := "dbnd_jvm_sanity_check",
        libraryDependencies ++= Seq(
            "ai.databand" % "dbnd-api-deequ" % databandVersion,
            "org.slf4j" % "slf4j-api" % "1.7.16"
        ),
        mainClass in Compile := Some("ai.databand.SanityCheck")
    )

assemblyJarName in assembly := "dbnd_jvm_sanity_check-assembly-latest.jar"
