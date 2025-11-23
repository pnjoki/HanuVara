import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.cassandra._

object UserAuthProcessor {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaSparkCassandraAuth")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost") // Point to your Docker Cassandra
      .getOrCreate()

    import spark.implicits._

    // 1. DEFINE SCHEMAS
    val kafkaBootstrap = "localhost:9092"

    // 2. REGISTRATION FLOW
    // Read from Kafka 'register_topic'
    val rawRegStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", "register_topic")
      .load()

    val regDS = rawRegStream.selectExpr("CAST(value AS STRING)")
      .select(get_json_object($"value", "$.email").alias("email"))
      .withColumn("otp", (rand() * 9000 + 1000).cast("int").cast("string")) // Generate 4 digit OTP
      .withColumn("is_verified", lit(false))
      .withColumn("created_at", current_timestamp())

    // Process Registration: Write to Cassandra AND Simulate Sending Email
    val regQuery = regDS.writeStream
      .foreachBatch { (batchDF, batchId) =>
        if (!batchDF.isEmpty) {
          // A: Write to Cassandra
          batchDF.write
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "users", "keyspace" -> "messaging_app"))
            .mode("append")
            .save()

          // B: Simulate Email Sending (Write back to a 'feedback' topic)
          val emailNotifications = batchDF.selectExpr(
             "to_json(struct(email, otp, 'OTP Sent to Email' as status)) as value"
          )
          emailNotifications.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("topic", "client_responses")
            .save()

          println(s"Batch $batchId: Users registered and OTPs generated.")
        }
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // 3. OTP VALIDATION FLOW
    // Read from Kafka 'verify_topic'
    val rawVerifyStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", "verify_topic")
      .load()

    val verifyDS = rawVerifyStream.selectExpr("CAST(value AS STRING)")
      .select(
        get_json_object($"value", "$.email").alias("req_email"),
        get_json_object($"value", "$.otp").alias("req_otp")
      )

    val verifyQuery = verifyDS.writeStream
      .foreachBatch { (batchDF, batchId) =>
        if (!batchDF.isEmpty) {
          // Load current state of Cassandra to check OTP
          val usersSnapshot = spark.read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "users", "keyspace" -> "messaging_app"))
            .load()

          // Join request with DB
          val joined = batchDF.join(usersSnapshot,
             batchDF("req_email") === usersSnapshot("email") &&
             batchDF("req_otp") === usersSnapshot("otp"),
             "left"
          )

          val result = joined.withColumn("status",
            when($"email".isNotNull, "Verified Success").otherwise("Verification Failed"))
            .selectExpr("to_json(struct(req_email as email, status)) as value")

          // Send result back to Client via Kafka
          result.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("topic", "client_responses")
            .save()

           // Optional: Update Cassandra 'is_verified' to true here (omitted for brevity)
        }
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    spark.streams.awaitAnyTermination()
  }
}