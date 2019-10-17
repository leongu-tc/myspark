package leongu.myspark._business.rt_asset

import java.util.Date

import leongu.myspark._business.rt_asset.adjusting.AdjustingBolt
import leongu.myspark._business.rt_asset.adjusting.AdjustingBolt.ADJUSTING_DAY
import leongu.myspark._business.rt_asset.util.{ExternalTools, RTACons, RTARules}
import leongu.myspark.session.streaming.KafkaExample._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.yaml.snakeyaml.Yaml

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object RTAsset extends Logging with RTACons {
  //  Logger.getLogger("org").setLevel(Level.WARN)
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

  def loadKafkaSource(spark: SparkSession): DataFrame = {
    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getOrElse("kafka_servers", "localhost:9092").toString)
      .option("kafka.kafka.security.authentication.sdp.publickey", conf.getOrElse("kafka_pub", "").toString)
      .option("kafka.kafka.security.authentication.sdp.privatekey", conf.getOrElse("kafka_pri", "").toString)
      .option("failOnDataLoss", "false") //参数 数据丢失，false表示工作不被禁止，会从checkpoint中获取找到断电，从断点开始从新读数据
      .option("max.poll.records", 10000)
      .option("subscribe", topic)
    if (conf.getOrElse("kafka_pri", "").toString.length > 0) {
      reader.option("kafka.security.protocol", "SASL_SDP")
      reader.option("kafka.sasl.mechanism", "SDP")
    }
    reader.load()
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
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    readConf(confFileName)

    // 1 adjusting
    AdjustingBolt.initialAdjust(spark, conf)

    // 2 realtime computing
    val df = loadKafkaSource(spark)
    proc(spark, df)


    println("Over!")
  }

  /**
    * compute realtime asset
    *
    * @param spark
    * @param df
    */
  def proc(spark: SparkSession, df: DataFrame): Unit = {
    logInfo("Starting processing streaming data ... ...")
    val day = conf.getOrElse(ADJUSTING_DAY, "20190902").toString
    import spark.implicits._
    val lineRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]

    val fields = match_schema.toList.map(field => StructField(field._1, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = lineRDD
      .map(_.split(","))
      .map(attributes => Row.fromSeq(attributes)).rdd
    val rowDf = spark.createDataFrame(rowRDD, schema)
      .withWatermark("ts", "300 seconds")
      .dropDuplicates()
      .where(s"trddate > $day") // if we lost checkpoints, we consume kafka from the earlist
      .cache()
    // 1 stk asset rt
    // fundid,secuid,orgid,market,stkcode,serverid
    val stkDf = rowDf.select("fundid", "secuid", "orgid", "market", "stkcode", "serverid",
      "bsflag", "matchqty", "matchtype", "matchsno", "trddate")
      .where($"bsflag".isin(RTARules.BS_FLAG_STK: _*))
      .where("bsflag in ('03', '04') or matchtype == 0")

    val stk_query = stkDf.writeStream
      .queryName("rtstkasset_compute")
      .outputMode("append")
      .foreachBatch { (df: DataFrame, bid: Long) =>
        df.foreachPartition(records => {
          //          println(s"create hbase client ------------------- start, bid = $bid")
          val hbase = ExternalTools.getHBase(conf)
          var hbaseConf = hbase._1
          var connection = hbase._2
          var table = hbase._3
          //          println("create hbase client ------------------- start")
          records.foreach(record => {
            println(record.toString)
            // 1 read hbase
            val rk = record.getString(0).concat("_").concat(record.getString(1))
              .concat("_").concat(record.getString(2))
              .concat("_").concat(record.getString(3))
              .concat("_").concat(record.getString(4))
              .concat("_").concat(record.getString(5))
            val vals = ExternalTools.getHBaseVal(table, rk, Seq("stkbal"))
            val rt_stkbal = RTARules.stk_compute(record.getString(6), vals(0), record.getLong(7))
            val rt_matchsno = record.getLong(9)
            val rt_trddate = record.getLong(10)
            // 2 write hbase
            val theput = new Put(Bytes.toBytes("rk_" + record.get(0).toString))
            theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("stkbal"), Bytes.toBytes(rt_stkbal))
            theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("matchsno"), Bytes.toBytes(rt_matchsno))
            theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("trddate"), Bytes.toBytes(rt_trddate))
            table.put(theput)
          })
          //          println("close hbase connection -----------------")
          connection.close()
        })
      }
      .option("checkpointLocation", "/tmp/checkpoints/rtasset")
      .start()

    stk_query.awaitTermination(HALF_DAY)
    logInfo("Spark Session close at " + new Date + " ... ...")
    spark.close();
  }

}
