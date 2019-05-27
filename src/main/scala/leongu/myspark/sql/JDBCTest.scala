package leongu.myspark.sql

import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object JDBCTest extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // IDE 内启动
      .master("spark://localhost:7077")
      //      .master("local")
      .appName("Spark JDBC Example")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/dcep")
      .option("dbtable", "dcep.tbl1")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234567")
      .load()

    jdbcDF.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "1234567")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "name VARCHAR(64), num int")
      .jdbc("jdbc:mysql://localhost:3306/dcep", "dcep.tbl2", connectionProperties)
    spark.stop()
    println("done!")
  }

}
