import sbt._
import Dependencies._

val scioVersion = "0.12.7"
val beamVersion = "2.38.0"

ThisBuild / resolvers += "confluent" at "https://packages.confluent.io/maven/"

addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")

lazy val commonSettings = Def.settings(
  organization := "com.fgb.pipeline",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.3",
  scalacOptions ++= Seq("-target:11", "-deprecation", "-feature", "-unchecked", "-Ymacro-annotations"),
  javacOptions ++= Seq("-source", "11", "-target", "11"),
  resolvers += "Confluent" at "https://packages.confluent.io/maven/"
)

// code generation from proto files
lazy val protobufSettings = Seq(
  PB.protocVersion := "3.17.3",
  Compile / PB.targets := Seq(
    scalapb.gen(javaConversions = false) -> (Compile / sourceManaged).value
  ),
  Compile / PB.protoSources ++= Seq(file("proto/poc/input"), file("proto/poc/output"))
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(protobufSettings)
  .settings(
    name := "spark-MQTT-poc",
    description := "Spark poc using Mqtt",
    Compile / mainClass := Some("pipelines.Main"),
    // run / fork := true,
    libraryDependencies ++= Libs.all
  )
  .enablePlugins(JavaAppPackaging)
