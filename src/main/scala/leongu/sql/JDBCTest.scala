package leongu.sql

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

    spark.stop()
    println("done!")
  }

}
