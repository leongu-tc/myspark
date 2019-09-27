package leongu.myspark.session.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object KafkaExample extends Logging {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {
    // For windows
    // System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop-common-2.2.0-bin-master")

    Logger.getLogger("org").setLevel(Level.ERROR)


    System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop-common-2.2.0-bin-master")

    val spark = SparkSession
      .builder()
      // 指定spark集群
      //      .master("spark://localhost:7077")
      .master("local")
      .appName("Kafka example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      //      .option("kafka.bootstrap.servers", "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667")
      //      .option("kafka.kafka.security.authentication.sdp.publickey", "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO")
      //      .option("kafka.kafka.security.authentication.sdp.privatekey", "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB")
      //      .option("kafka.security.protocol", "SASL_SDP")
      //      .option("kafka.sasl.mechanism", "SDP")
      .option("failOnDataLoss", "false") //参数 数据丢失，false表示工作不被禁止，会从checkpoint中获取找到断电，从断点开始从新读数据
      .option("max.poll.records", 10000)
      .option("subscribe", "topic1")
      .load()

    df.printSchema()

    kafkatopic(spark, df);

    println("done!")
  }

  def kafkatopic(spark: SparkSession, df: DataFrame): Unit = {
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
      .option("checkpointLocation", "checkpoints") // HDFS 保存 checkpoint 的方式
      .start()

    //    val query = rowdf.writeStream
    //      .outputMode("append").format("text").option("path", "/test/zdb/result")
    //      .option("checkpointLocation", "checkpoints") // HDFS 保存 checkpoint 的方式
    //      .trigger(Trigger.ProcessingTime(
    //      300
    //    )).start()

    query.awaitTermination()
  }

  def kafkatokafkatopic(spark: SparkSession, df: DataFrame): Unit = {
    val query = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "topic2")
      //      .option("kafka.bootstrap.servers", "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667")
      //      .option("kafka.kafka.security.authentication.sdp.publickey", "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO")
      //      .option("kafka.kafka.security.authentication.sdp.privatekey", "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB")
      //      .option("kafka.security.protocol", "SASL_SDP")
      //      .option("kafka.sasl.mechanism", "SDP")
      .option("checkpointLocation", "checkpoints")
      .start()

    query.awaitTermination()
  }

}
