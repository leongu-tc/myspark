package leongu.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredStreamingExample extends Logging {

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

    basicdf(spark);

    println("done!")
  }

  def socketsource(spark: SparkSession): Unit = {
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    lines.printSchema()

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

  def filesource(spark: SparkSession): Unit = {
    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("file:///Users/apple/workspaces/sparks/myspark/src/main/resources/csv") // Equivalent to format("csv").load("/path/to/directory")
    // execute while new file created in the path

    csvDF.printSchema()

    // Start running the query that prints the running counts to the console
    val query = csvDF.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def basicdf(spark: SparkSession): Unit = {
    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer").add("job", "string")
    val df: DataFrame = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("file:///Users/apple/workspaces/sparks/myspark/src/main/resources/csv") // Equivalent to format("csv").load("/path/to/directory")
    // execute while new file created in the path

    // Select the devices which have signal more than 10
    df.select("name").where("age > 30") // using untyped APIs

    // Running count of the number of updates for each device type
    val cntDf = df.groupBy("job").count() // using untyped API
    // ---------------------------------------------
    //     Start running the query that prints the running counts to the console
    val query = cntDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def basicds(spark: SparkSession): Unit = {
    val userSchema = new StructType().add("name", "string").add("age", "integer").add("job", "string")
    val df: DataFrame = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)
      .csv("file:///Users/apple/workspaces/sparks/myspark/src/main/resources/csv") // Equivalent to format("csv").load("/path/to/directory")
    // execute while new file created in the path

    import spark.implicits._
    val ds: Dataset[Person] = df.as[Person]

    ds.filter(_.age.map(_ > 30).getOrElse(false)).map(_.name)// using typed APIs
//    ds.filter(_.age > 30).map(_.name) // using typed APIs

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    var cntDf = ds.groupByKey(_.job).agg(typed.avg(_.age.get))
//    var cntDf = ds.groupByKey(_.job).agg(typed.avg(_.age))

    // ---------------------------------------------

    // Start running the query that prints the running counts to the console
    val query2 = cntDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query2.awaitTermination()
  }

}
