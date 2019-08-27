package leongu.myspark.streaming.sdp

;

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SDPKafka2Kafka extends Logging {

  case class SDPKafka2Kafka(name: String, age: Long)

  def main(args: Array[String]) {
    // for windows TODO
    // System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop-common-2.2.0-bin-master")

    args.map(logInfo(_))
    val spark = SparkSession
      .builder()
      // IDE 内启动
      //      .master("spark://localhost:7077")
      //      .master("local")
      .appName("SDP Kafka example")
      .getOrCreate()

    kafkatokafkatopic(spark, "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO", "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB");

    println("done!")
  }

  def kafkatokafkatopic(spark: SparkSession, pubkey: String, prikey: String): Unit = {
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667")
      .option("subscribe", "person")
      .option("kafka.kafka.security.authentication.sdp.publickey", pubkey)
      .option("kafka.kafka.security.authentication.sdp.privatekey", prikey)
      .option("kafka.security.protocol", "SASL_SDP")
      .option("kafka.sasl.mechanism", "SDP")
      .load()

    df.printSchema()

    val query = df.writeStream
      .format("kafka")
      .option("topic", "person2")
      .option("checkpointLocation", "checkpoints")
      .option("kafka.bootstrap.servers", "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667")
      .option("kafka.kafka.security.authentication.sdp.publickey", pubkey)
      .option("kafka.kafka.security.authentication.sdp.privatekey", prikey)
      .option("kafka.security.protocol", "SASL_SDP")
      .option("kafka.sasl.mechanism", "SDP")
      .start()

    query.awaitTermination()
  }
}
