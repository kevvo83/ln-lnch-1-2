import sbt._

name := "ln-lnch-1"
version := "0.1"
scalaVersion := "2.13.1"

val root = (project in file("."))

libraryDependencies ++= Seq (
  // Testing Libraries
  "junit" % "junit" % "4.12" % Test,
  //"org.scalatest" %% "scalatest" % "3.0.3" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0",

  // Kafka Streams dependencies for microservices development
  "org.apache.kafka" % "kafka-streams" % "2.3.0",
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.3.0" % Test,

  // Kafka Client dependencies
  "org.apache.kafka" % "kafka-clients" % "2.3.0",

  // logging
  "org.slf4j" % "slf4j-log4j12" % "1.7.28",

  // Schema Registry client, Avro Serde classes
  "io.confluent" % "kafka-schema-registry-client" % "3.3.0" withSources() withJavadoc(),
  "io.confluent" % "kafka-avro-serializer" % "3.3.0" withSources() withJavadoc(),
  "io.confluent" % "kafka-streams-avro-serde" % "5.3.0" withSources() withJavadoc()
)

resolvers += "confluent" at "https://packages.confluent.io/maven/"