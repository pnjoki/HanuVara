name := "SparkKafkaAuth"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  // Core Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Connector for Kafka (Structured Streaming)
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Connector for Cassandra
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.0",

  // Logging (Optional but helpful to suppress verbose logs)
  "ch.qos.logback" % "logback-classic" % "1.2.11"
)

// Configuration to handle duplicate file errors during assembly (if you create a fat JAR)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}