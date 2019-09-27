package leongu.myspark.session.streaming

import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

object JoinExample extends Logging {

  case class Person(name: String, salary: Long, job: String)

  case class Person1(name1: String, salary: Long, job: String, time1: Timestamp)

  case class Person2(name2: String, age: Long, time2: Timestamp)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // 指定spark集群
      .master("spark://localhost:7077")
      //      .master("local")
      .appName("Join Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    streamJoinStream(spark);

    println("done!")
  }

  def streamJoinStatic(spark: SparkSession): Unit = {
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


  def streamJoinStream(spark: SparkSession): Unit = {

    val streamingDf1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

    val streamingDf2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic2")
      .load()

    import spark.implicits._
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]

    val lineRDD1: Dataset[String] = streamingDf1.selectExpr("CAST(value AS STRING)").as[String]
    //处理成Row
    val ds = lineRDD1.map(_.split(","))
      .map(attributes => {
        try {
          Person1(attributes(0), attributes(1).trim.toInt, attributes(2), Timestamp.valueOf(attributes(3)))
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person1("Nil", 0, "Nil", Timestamp.valueOf("1970-01-01 00:00:00"))
          }
        }
      })

    val lineRDD2: Dataset[String] = streamingDf2.selectExpr("CAST(value AS STRING)").as[String]
    //处理成Row
    val ds2 = lineRDD2.map(_.split(","))
      .map(attributes => {
        try {
          Person2(attributes(0), attributes(1).trim.toInt, Timestamp.valueOf(attributes(2)))
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person2("Nil", 0, Timestamp.valueOf("1970-01-01 00:00:00"))
          }
        }
      })

    val rowdf = ds.toDF()
    val rowdf2 = ds2.toDF()
    rowdf.printSchema()
    rowdf2.printSchema()

    // Apply watermarks on event-time columns
    val rowdfWithWatermark = rowdf.withWatermark("time1", "10 seconds")
    val rowdfWithWatermark2 = rowdf2.withWatermark("time2", "15 seconds")

    val col = functions.expr("name1 = name2 AND time1 >= time2 AND time1 < time2 + interval 20 second")
    // Join with event-time constraints
    val join = rowdfWithWatermark.join(rowdfWithWatermark2, col, joinType = "leftOuter")

    // Start running the query that prints the running counts to the console
    val query = join.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
