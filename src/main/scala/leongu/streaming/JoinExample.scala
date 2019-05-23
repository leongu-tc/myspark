package leongu.streaming

import java.sql.Timestamp

import leongu.Constants
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object JoinExample extends Logging {

  case class Person(name: String, age: Option[Long], job: String)

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
    val staticDf = spark.read.json("file://" + Constants.prefix + "people.json")
    val streamingDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

//    val line = streamingDf.selectExpr("CAST(value AS STRING)").as[String]
//    line.flatMap(_.split(" ")).toDF("")



    val join = streamingDf.join(staticDf, "value") // inner equi-join with a static DF

    // Start running the query that prints the running counts to the console
    val query = join.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
