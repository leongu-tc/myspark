package leongu.myspark.basic

/* SimpleApp.scala */
import leongu.myspark._business.util.Utils
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    Utils.readConfFromResources("/mac.yaml")
//    val logFile = "/user/apple/README.md" // Should be some file on your system
    val logFile = "/app-logs/admin/logs/application_1577254247165_0139"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"--------Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}