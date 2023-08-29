package Main

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.common.serialization.StringDeserializer

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SparkStreamingApp")
      .master("local[*]") // Set the Spark master URL
      .getOrCreate()

    // Create a StreamingContext
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    // Kafka configuration
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", // Replace with your Kafka server address
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ThreatIntelligenceSolution",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("IpAddresses") // Replace with your Kafka topic name

    // Create a Kafka DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )


    // Extract IP addresses from log messages
    val logStream = kafkaStream.flatMap(_.value.split("\\s+"))
      .filter(ip => ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"))

    // Print the log messages
    logStream.foreachRDD { rdd =>
      println("Received log messages: " + rdd.count())
      rdd.foreach(println)
    }

    // Read the IP address data from MySQL table
    val jdbcUrl = "jdbc:mysql://localhost:3306/malicious_ip_db"
    val username = "rugupta82"
    val password = "********"
    val sourceTableName = "malicious_ip_address"
    val targetTableName = "matched_ip"

    val ipAddressDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("user", username)
      .option("password", password)
      .option("dbtable", sourceTableName)
      .load()

    // Perform IP address matching
    val matchedIPs = logStream.transform { rdd =>
      val logDF = spark.createDataFrame(rdd.map(Row(_)), ipAddressDF.schema)
      logDF.join(ipAddressDF, Seq("ip_address"), "inner")
        .select("ip_address")
        .rdd
        .map(row => row.getString(0))
    }

    // Show the matched IP addresses
    matchedIPs.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreach(println)
      }
    }

    // Store the matched IP addresses in a different table within the same database
    matchedIPs.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val df = spark.createDataFrame(rdd.map(Row(_)), ipAddressDF.schema)
        try {
          df.write.format("jdbc")
            .option("url", jdbcUrl)
            .option("user", username)
            .option("password", password)
            .option("dbtable", targetTableName)
            .mode("append")
            .save()
        } catch {
          case e: Exception => e.printStackTrace()
        }

      }
    }

    // Start the streaming context
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
