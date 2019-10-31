package leongu.myspark.session.api

import leongu.myspark.util.myutil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ReaderWriterApi {
  Logger.getLogger("org").setLevel(Level.WARN)

  case class Person(name: String, gender: String, age: Int, addr: String, col1: String, id: Int)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("dfwriter_api")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.sql.warehouse.dir", "/user/spark/spark-warehouse") // 不论warehouse在哪里hive都可以访问其数据
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("SELECT * FROM db1.t3")
    df.show()

    builtIn(spark, df)

    println("Over!")
  }

  def builtIn(spark: SparkSession, df: DataFrame) = {
    val localPath = "/tmp/spark/reader"
/*
    /** CSV */
    myutil.deleteDir(localPath)
    // mode:'overwrite', 'append', 'ignore', 'error', 'errorifexists'
    df.write.mode(SaveMode.Append).format("csv").save(localPath)
    spark.read.csv(localPath).show()

    /** default: Parquet */
    myutil.deleteDir(localPath)
    df.write
      .partitionBy("id")
      .save(localPath)
    spark.read.parquet(localPath).show() // spark.sql.sources.default 默认是parquet
*/
    /** bucket save as table */
    spark.sql("DROP TABLE IF EXISTS db1.t3_parquet").show()
    df.write
      .partitionBy("id")
      .bucketBy(1, "name") // save不支持bucket，只能用saveAsTable
      .saveAsTable("db1.t3_parquet")
    spark.sql("select * from db1.t3_parquet").show()
    // DESCRIBE EXTENDED db1.t3_parquet 数据目录在 hdfs的 /tmp/hive/warehouse/t3_parquet, 必须创建session之前先设置
    // spark.conf.set("spark.sql.warehouse.dir", "/user/spark/spark-warehouse")

  }
}
