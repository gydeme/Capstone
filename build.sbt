import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / organization := "ch.epfl.lts2"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"


lazy val root = (project in file("."))
  .settings(
    name := "SparkWiki",
    resolvers += "mvnrepository" at "https://mvnrepository.com/artifact/",
    resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.4.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1",
    libraryDependencies += "org.rogach" %% "scallop" % "4.1.0", // Consolidated Scallop version
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
    libraryDependencies += "org.scalanlp" %% "breeze" % "2.0",
    libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "5.3.2_for_spark_3",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.5.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.3",
    libraryDependencies += "com.google.guava" % "guava" % "31.1-jre",
    libraryDependencies += "com.github.servicenow.stl4j" % "stl-decomp-4j" % "1.0.5",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test,
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.32",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.1",

    scalacOptions += "-deprecation",
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens", "java.base/java.nio=ALL-UNNAMED",
      "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    assembly / mainClass := Some("ch.epfl.lts2.wikipedia.DumpProcessor"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

