package leongu.streaming

import leongu.Constants
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object JoinExample extends Logging {

  case class Person(name: String, salary: Long, job: String)

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
    val staticDf = spark.read.json("file:///Users/apple/workspaces/sparks/myspark/src/main/resources/people.json")
    val streamingDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()
    staticDf.printSchema()
    import spark.implicits._
    val lineRDD: Dataset[String] = streamingDf.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    //处理成Row
    val ds = lineRDD.map(_.split(","))
      .map(attributes => {
        try {
          Person(attributes(0), attributes(1).trim.toInt, attributes(2))
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person("Nil", 0, "Nil")
          }
        }
      })

    val rowdf = ds.toDF()
    rowdf.printSchema()


    val join = rowdf.join(staticDf, "name") // inner equi-join with a static DF

    // Start running the query that prints the running counts to the console
    val query = join.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
