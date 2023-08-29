package Main

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object DynamicIPmatch {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("IPAddressMatcher")
      .master("local[*]") // Set the Spark master URL
      .getOrCreate()

    // Read the IP address data from MySQL table
    val jdbcUrl = "jdbc:mysql://localhost:3306/malicious_ip_db"
    val username = "rugupta82"
    val password = "*********S"
    val sourceTableName = "malicious_ip_address"
    val targetTableName = "matched_ip"

    val ipAddressDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("user", username)
      .option("password", password)
      .option("dbtable", sourceTableName)
      .load()


    // Read the IP addresses to match from a text file
    val ipAddressFile = "C:\\Users\\ruchi\\IdeaProjects\\UpdatedThreatIntelligence\\src\\malicious_ip_4.txt"

    val ipAddresses = spark.read.textFile(ipAddressFile)
      .map(_.trim)(Encoders.STRING)
      .filter(_.nonEmpty)

    // Perform IP address matching
    val matchedIPs = ipAddressDF.join(ipAddresses, ipAddressDF("ip_address") === ipAddresses("value"), "inner")
      .select(ipAddressDF("ip_address"))

    // Show the matched IP addresses
    matchedIPs.show()

    // Store the matched IP addresses in a different table within the same database
    matchedIPs.write.format("jdbc")
      .option("url", jdbcUrl)
      .option("user", username)
      .option("password", password)
      .option("dbtable", "matched_ip")
      .mode("append")
      .save()

    // Stop the SparkSession
    spark.stop()
  }
}

