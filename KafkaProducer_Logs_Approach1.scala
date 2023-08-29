package Main

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConverters._

object KafkaProducer_Logs {
  def main(args: Array[String]): Unit = {
    val inputFile = "C:\\Users\\ruchi\\IdeaProjects\\ThreatIntelligence_DataPipeline\\src\\malicious_ip_4.txt"
    val kafkaTopic = "IpAddresses"
    val kafkaServers = "localhost:9092" // Replace with your Kafka server address

    // Kafka producer configuration
    val props = new Properties()
    props.put("bootstrap.servers", kafkaServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create Kafka producer
    val producer = new KafkaProducer[String, String](props)

    // Read IP addresses from the text file
    val ips = Files.readAllLines(Paths.get(inputFile)).asScala

    // Send each IP address as a log message to the Kafka topic
    ips.foreach { ip =>
      val log = s"""{"ip":"$ip"}"""
      val record = new ProducerRecord[String, String](kafkaTopic, log)
      producer.send(record)
    }

    // Close the producer
    producer.close()
  }
}



