package leongu.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object StructuredStreamingExample extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // IDE 内启动
      .master("spark://localhost:7077")
      //      .master("local")
      .appName("Structured Streaming example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    wordcount(spark);

    println("done!")
  }

  def wordcount(spark: SparkSession): Unit = {
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
