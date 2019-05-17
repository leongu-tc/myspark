package leongu.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object KafkaExample extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // IDE 内启动
      .master("spark://localhost:7077")
      //      .master("local")
      .appName("Kafka example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    kafkatopic(spark);

    println("done!")
  }

  def kafkatopic(spark: SparkSession): Unit = {
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    val ds = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "topic2")
      .option("checkpointLocation","checkpoints")
      .start()

    ds.awaitTermination()
  }

}
