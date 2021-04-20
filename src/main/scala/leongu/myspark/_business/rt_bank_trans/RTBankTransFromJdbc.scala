package leongu.myspark._business.rt_bank_trans

import java.util.Date

import leongu.myspark._business.rt_asset.adjusting.AdjustingBolt
import leongu.myspark._business.rt_asset.rt.RTAProcessor
import leongu.myspark._business.rt_asset.util.{ExternalTools, RTACons}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object RTBankTransFromJdbc extends Logging with RTACons {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("rt_bank_trans")
//      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 否则访问hbase失败因为对象不能序列化
//      .enableHiveSupport()
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    // 2 realtime computing
    proc(spark)

    println("Over!")
  }

  /**
    * compute realtime asset
    *
    * @param spark
    */
  def proc(spark: SparkSession): Unit = {
    logInfo("Starting processing streaming data ... ...")
    val jdbcOptions = Map(
      "user" -> "root",
      "password" -> "1234567",
      "database" -> "db1",
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> "jdbc:mysql://localhost:3306/db1?characterEncoding=utf8&useSSL=false"
    )

    // Create DataFrame representing the stream of input lines from jdbc
    val stream = spark.readStream
//      .format("jdbc-streaming")
      .format("org.apache.spark.sql.execution.streaming.sources.JDBCStreamSourceProvider")
      .options(jdbcOptions +
        ("dbtable" -> "table1") +
        ("offsetColumn" -> "bizdate * 1000000 + sno:long") +
        ("startingoffset" -> "20210102000003"))
      .load

    // Start running the query that prints 'select result' to the console
    val query = stream.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "checkpoints")
      .start()
    query.awaitTermination(HALF_DAY)
    logInfo("Spark Session close at " + new Date + " ... ...")
    spark.close();
  }

}
