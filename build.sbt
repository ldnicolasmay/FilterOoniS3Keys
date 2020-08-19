name := "FilterOoniS3Keys"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "com.typesafe" % "config" % "1.3.0",
  "com.lihaoyi" %% "os-lib" % "0.2.9"
)
