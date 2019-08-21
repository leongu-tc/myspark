package leongu.myspark.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SDPKafkaExample extends Logging {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      // IDE 内启动
//      .master("spark://localhost:7077")
      //      .master("local")
      .appName("SDP Kafka example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    kafkatopic(spark, args(0), args(1));

    println("done!")
  }

  def kafkatopic(spark: SparkSession, pubkey: String, prikey: String): Unit = {
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667")
      .option("subscribe", "person")
      .option("kafka_security_authentication_sdp_publickey", pubkey)
      .option("kafka_security_authentication_sdp_privatekey", prikey)
      .load()

    df.printSchema()

    import spark.implicits._
    val lineRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    //处理成Row
    val ds = lineRDD.map(_.split(","))
      .map(attributes => {
        try {
          Person(attributes(0), attributes(1).trim.toInt)
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person("Nil", 0)
          }
        }
      })

    val rowdf = ds.toDF()
    rowdf.printSchema()

    // Start running the query that prints the running counts to the console
    val query = rowdf.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
