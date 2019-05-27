package leongu.myspark.streaming

import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DeduplicationExample extends Logging {

  case class Person(name: String, age: Long, time: Timestamp)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // IDE 内启动
      .master("spark://localhost:7077")
      //      .master("local")
      .appName("Deduplication example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    withwm(spark);

    println("done!")
  }

  def withwm(spark: SparkSession): Unit = {
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic2")
      .load()

    df.printSchema()

    import spark.implicits._
    val lineRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    //处理成Row
    val ds = lineRDD.map(_.split(","))
      .map(attributes => {
        try {
          Person(attributes(0), attributes(1).trim.toInt, Timestamp.valueOf(attributes(2)))
        }
        catch {
          case e1: Exception => {
            logInfo("ex: " + attributes(0) + attributes(1) + attributes(2), e1)
            println(attributes)
            Person("Nil", 0, Timestamp.valueOf("1970-01-01 00:00:00"))
          }
        }
      })

    val rowdf = ds.toDF()
    rowdf.printSchema()

    val ret = rowdf.withWatermark("time", "10 seconds")
      .dropDuplicates("name")
    // Start running the query that prints the running counts to the console
    val query = ret.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
