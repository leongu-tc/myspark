package leongu.myspark.session.api

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSetTests extends Logging {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {
    args.map(logInfo(_)) // log all args
    val spark = SparkSession
      .builder()
      //      .master("local")
      .config("hadoop.security.authentication.sdp.publickey", "OJ1wm30JncacE0eY9BwquG47eO63pTh0wWvb")
      .config("hadoop.security.authentication.sdp.privatekey", "hUYHWnK4lBUzSNncGO55pBB8ltLf0x6n")
      .config("hadoop.security.authentication.sdp.username", "hdfs")
      .appName("SDPHdfs2Console example")
      .getOrCreate()

    // PRINT ALL config
    spark.conf.getAll.map(println _)

    jsonSource(spark);

    println("done!")
  }

  def jsonSource(spark: SparkSession): Unit = {
    val peopleDF = spark.read.format("json").load("hdfs://hdfsCluster/user/spark/my/people.json")
    peopleDF.show()
    peopleDF.select("name", "age").write.format("parquet").save("file:///tmp/namesAndAges.parquet")
  }

  def csvSource(spark: SparkSession): Unit = {
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("hdfs://hdfsCluster/user/spark/my/people.csv")
    peopleDFCsv.show()
    peopleDFCsv.select("name", "age").write.mode(SaveMode.Overwrite).format("json").save("file:///tmp/namesAndAges2.json")
  }


  def sqlSource(spark: SparkSession): Unit = {
    val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://hdfsCluster/user/spark/my/users.parquet`")
    sqlDF.show()
  }

  def parquetSource(spark: SparkSession): Unit = {
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("hdfs://hdfsCluster/user/spark/my/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
  }

  def mergeSchema(spark: SparkSession): Unit = {
    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("/tmp/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("/tmp/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("/tmp/test_table")
    mergedDF.printSchema()

    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths
    // root
    //  |-- value: int (nullable = true)
    //  |-- square: int (nullable = true)
    //  |-- cube: int (nullable = true)
    //  |-- key: int (nullable = true)
    mergedDF.show()
  }
}
