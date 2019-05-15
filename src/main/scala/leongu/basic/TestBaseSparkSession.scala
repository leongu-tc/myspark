package leongu.basic

import org.apache.spark.sql.SparkSession

object TestBaseSparkSession {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.json("/Users/apple/workspaces/sparks/spark/examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // For implicit conversions like converting RDDs to DataFrames
    println("done!")
  }
}
