package Main

import java.io.InputStream
import java.net.URL
import scala.io.Source
import java.sql.{Connection, DriverManager, PreparedStatement}

object URLdataIngest {
  def main(args: Array[String]): Unit = {
    // URL of the data
    val url = "https://raw.githubusercontent.com/BlancRay/Malicious-ip/master/ips"

    // Connect to the MySQL database
    val jdbcUrl = "jdbc:mysql://localhost:3306/malicious_ip_db"
    val username = "rugupta82"
    val password = "********"
    Class.forName("com.mysql.cj.jdbc.Driver")
    val connection: Connection = DriverManager.getConnection(jdbcUrl, username, password)

    try {
      // Read the data from the URL
      val inputStream: InputStream = new URL(url).openStream()
      val data: String = Source.fromInputStream(inputStream).mkString

      // Parse the data and store it in the database
      val insertQuery = "INSERT INTO malicious_ip_address (ip_address) VALUES (?)"
      val preparedStatement: PreparedStatement = connection.prepareStatement(insertQuery)

      data.split("\\s+").foreach { ipAddress =>
        val truncatedIpAddress = ipAddress.substring(0, Math.min(ipAddress.length, 20)) // Adjust the length as per your needs
        preparedStatement.setString(1, truncatedIpAddress)
        preparedStatement.executeUpdate()
      }


      // Close the prepared statement and connection
      preparedStatement.close()
      connection.close()

      println("Data inserted successfully!")
    } catch {
      case ex: Exception =>
        println("An error occurred: " + ex.getMessage)
    }
  }

}
