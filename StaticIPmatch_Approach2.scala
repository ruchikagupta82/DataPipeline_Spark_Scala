package Main

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StaticIPmatch {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Static_IPMatcher")
      .master("local[*]") // Set the Spark master URL
      .getOrCreate()

    // Read the IP address data from MySQL table
    val jdbcUrl = "jdbc:mysql://localhost:3306/malicious_ip_db"
    val username = "rugupta82"
    val password = "*********"
    val sourceTableName = "malicious_ip_address"
    val targetTableName = "matched_ip_static"

    val ipAddressDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("user", username)
      .option("password", password)
      .option("dbtable", sourceTableName)
      .load()

    // Hardcoded IP addresses
    val ipAddresses = Seq(
      "88.206.91.88",
      "14.177.253.227",
      "59.96.137.217",
      "113.173.128.54",
      "59.95.214.61",
      "124.225.44.0/24",
      "124.227.31.0/24",
      "124.235.138.0/24",
      "124.88.112.0/24",
      "124.88.113.0/24",
      "124.88.55.0/24"
    )

    import spark.implicits._
    val ipAddressesDF = ipAddresses.toDF("ip_address")

    // Perform IP address matching
    val matchedIPs = ipAddressDF.join(ipAddressesDF, ipAddressDF("ip_address") === ipAddressesDF("ip_address"), "inner")
      .select(ipAddressDF("ip_address"))

    // Show the matched IP addresses
    matchedIPs.show()

    // Store the matched IP addresses in a different table within the same database
    matchedIPs.write.format("jdbc")
      .option("url", jdbcUrl)
      .option("user", username)
      .option("password", password)
      .option("dbtable", targetTableName)
      .mode("append")
      .save()

    // Stop the SparkSession
    spark.stop()
  }
}

