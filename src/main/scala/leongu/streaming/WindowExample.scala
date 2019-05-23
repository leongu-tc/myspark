package leongu.streaming

import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WindowExample extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // IDE 内启动
      .master("spark://localhost:7077")
      //      .master("local")
      .appName("Structured Streaming example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    socketsource(spark);

    println("done!")
  }

  def socketsource(spark: SparkSession): Unit = {
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true) //输出内容包括时间戳
      .load()

    lines.printSchema()

    import spark.implicits._
    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", "30 seconds", "10 seconds"),
      $"word"
    ).count()

    // Start running the query that prints the running counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
