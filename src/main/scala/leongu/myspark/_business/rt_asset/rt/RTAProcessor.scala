package leongu.myspark._business.rt_asset.rt

import leongu.myspark._business.rt_asset.RTAsset._
import leongu.myspark._business.rt_asset.util.{ExternalTools, RTARules}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object RTAProcessor extends Logging {
  def df_match(spark: SparkSession, df: DataFrame): DataFrame = {
    logInfo("Starting processing streaming data ... ...")
    val day = conf.getOrElse(ADJUSTING_DAY, "20190902").toString
    import spark.implicits._
    val recordRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]

    val fields = match_schema.toList.map(field => StructField(field._1, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = recordRDD
      .map(_.split(","))
      .map(attributes => Row.fromSeq(attributes)).rdd
    spark.createDataFrame(rowRDD, schema)
      .withWatermark("ts", "300 seconds")
      .dropDuplicates()
      .where(s"trddate > $day") // if we lost checkpoints, we consume kafka from the earlist
      .cache()
  }

  def start_stk_proc(spark: SparkSession, match_df: DataFrame): StreamingQuery = {
    import spark.implicits._
    // 1 stk asset rt
    // fundid,secuid,orgid,market,stkcode,serverid
    val stkDf = match_df.select("fundid", "secuid", "orgid", "market", "stkcode", "serverid",
      "bsflag", "matchqty", "matchtype", "matchsno", "trddate")
      .where($"bsflag".isin(RTARules.BS_FLAG_STK: _*))
      .where("bsflag in ('03', '04') or matchtype == 0")

    stkDf.writeStream
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
            val vals = ExternalTools.getHBaseVal(table, rk, Seq("stkbal", "bid"))
            if (vals(1) == bid) {
              // for Exactly Once Semantics
              logWarning(s"! Skipping repeated record, this might happen in a crush recovery, bid: $bid record: $record")
            } else {
              // 2 compute by rules
              val rt_stkbal = RTARules.stk_compute(record.getString(6), vals(0), record.getLong(7))
              val rt_matchsno = record.getLong(9)
              val rt_trddate = record.getLong(10)
              // 3 write to hbase
              val theput = new Put(Bytes.toBytes("rk_" + record.get(0).toString))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("stkbal"), Bytes.toBytes(rt_stkbal))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("matchsno"), Bytes.toBytes(rt_matchsno))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("trddate"), Bytes.toBytes(rt_trddate))
              table.put(theput)
            }
          })
          //          println("close hbase connection -----------------")
          connection.close()
        })
      }
      .option("checkpointLocation", "/tmp/checkpoints/rtasset/stk")
      .start()
  }

}
