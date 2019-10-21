package leongu.myspark._business.rt_asset

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

object RTAsset extends Logging with RTACons {
  Logger.getLogger("org").setLevel(Level.WARN)
  var conf: mutable.Map[String, Object] = mutable.Map()

  def readConf(file: String): Unit = {
    if (file != null) {
      val stream = Source.fromFile(file).reader()
      val yaml = new Yaml()
      var javaConf = yaml.load(stream).asInstanceOf[java.util.HashMap[String, Object]]
      conf = JavaConverters.mapAsScalaMapConverter(javaConf).asScala
    }

    logInfo("------------ Config -----------")
    conf.map(kv => logInfo(kv.toString()))
    logInfo("------------ End --------------")
  }

  def loadKafkaSource(spark: SparkSession, topic: String): DataFrame = {
    ExternalTools.getkafkaStreamReader(conf, spark, topic).load()
  }

  def main(args: Array[String]) {
    val confFileName = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => null
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("realtime_asset")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    readConf(confFileName)

    // 1 adjusting
    AdjustingBolt.initialAdjust(spark, conf)

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
    val match_df = RTAProcessor.df_match(spark, loadKafkaSource(spark, conf.getOrElse(KAFKA_TOPIC_MATCH, "match").toString))
    val logasset_df = RTAProcessor.df_match(spark, loadKafkaSource(spark, conf.getOrElse(KAFKA_TOPIC_LOG, "logasset").toString))
    // 1 stk rt asset
    val stk_query = RTAProcessor.start_stk_proc(spark, match_df)
    RTAProcessor.start_fund_proc(spark, match_df, logasset_df)
    stk_query.awaitTermination(HALF_DAY)
    logInfo("Spark Session close at " + new Date + " ... ...")
    spark.close();
  }

}
