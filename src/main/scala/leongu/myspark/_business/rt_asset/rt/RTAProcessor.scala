package leongu.myspark._business.rt_asset.rt

import leongu.myspark._business.rt_asset.RTAsset._
import leongu.myspark._business.rt_asset.util.{ExternalTools, RTARules}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RTAProcessor extends Logging {

  case class Match(
                    serverid: Long, matchsno: Long, operdate: Long, custid: Long, fundid: Long,
                    moneytype: String, fundkind: String, fundlevel: String, fundgroup: String, orgid: String,
                    brhid: String, secuid: String, rptsecuid: String, bsflag: String, rptbs: String,
                    matchtype: String, ordersno: String, orderid: String, market: String, stkcode: String,
                    stkname: String, stktype: String, trdid: String, orderprice: BigDecimal, bondintr: BigDecimal,
                    orderqty: Long, seat: String, matchtime: Long, matchprice: BigDecimal, matchqty: Long,
                    matchamt: BigDecimal, matchcode: String, clearamt: BigDecimal, operid: Long, operlevel: String,
                    operorg: String, operway: String, bankcode: String, bankbranch: String, banknetplace: String,
                    sourcetype: String, recnum: Long, bankorderid: String, bankid: String, exteffectamt: BigDecimal,
                    bankrtnflag: String, remark: String, creditid: String, creditflag: String, trddate: Long)


  def df_match(spark: SparkSession, df: DataFrame): DataFrame = {
    logInfo("Starting processing streaming data ... ...")
    import spark.implicits._
    val recordRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    recordRDD
      .map(_.split(","))
      .map(arr => Match(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3).toLong, arr(4).toLong,
        arr(5), arr(6), arr(7), arr(8), arr(9),
        arr(10), arr(11), arr(12), arr(13), arr(14),
        arr(15), arr(16), arr(17), arr(18), arr(19),
        arr(20), arr(21), arr(22), BigDecimal(arr(23)), BigDecimal(arr(24)),
        arr(25).toLong, arr(26), arr(27).toLong, BigDecimal(arr(28)), arr(29).toLong,
        BigDecimal(arr(30)), arr(31), BigDecimal(arr(32)), arr(33).toLong, arr(34),
        arr(35), arr(36), arr(37), arr(38), arr(39),
        arr(40), arr(41).toLong, arr(42), arr(43), BigDecimal(arr(44)),
        arr(45), arr(46), arr(47), arr(48), arr(49).toLong
      )
      ).toDF()
  }

  def start_stk_proc(spark: SparkSession, formatDf: DataFrame): StreamingQuery = {
    val day = conf.getOrElse(ADJUSTING_DAY, "20190902").toString
    formatDf.printSchema()
    formatDf //      .withWatermark("ts", "300 seconds")
      .dropDuplicates()
      .where(s"trddate > $day") // if we lost checkpoints, we consume kafka from the earlist
    import spark.implicits._
    // 1 stk asset rt
    // fundid,secuid,orgid,market,stkcode,serverid
    val stkDf = formatDf.select("fundid", "secuid", "orgid", "market", "stkcode", "serverid",
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
            val rk = record.get(0).toString.concat("_").concat(record.get(1).toString)
              .concat("_").concat(record.get(2).toString)
              .concat("_").concat(record.get(3).toString)
              .concat("_").concat(record.get(4).toString)
              .concat("_").concat(record.get(5).toString)
            val vals = ExternalTools.getHBaseVal(table, rk, Seq("stkbal", "bid"))
            if (vals(1) != null && ExternalTools.getHBaseLongVal(vals(1)) == bid) {
              // for Exactly Once Semantics
              logWarning(s"! Skipping repeated record, this might happen in a crush recovery, bid: $bid record: $record")
            } else {
              // 2 compute by rules
              var rt_stkbal = 0L
              if (vals(0) == null) {
                rt_stkbal = RTARules.stk_compute(record.getString(6), 0, record.getLong(7))
              } else {
                rt_stkbal = RTARules.stk_compute(record.getString(6), ExternalTools.getHBaseLongVal(vals(0)), record.getLong(7))
              }
              val rt_matchsno = record.getLong(9)
              val rt_trddate = record.getLong(10)
              // 3 write to hbase
              val theput = new Put(Bytes.toBytes(rk))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("stkbal"), Bytes.toBytes(rt_stkbal))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("matchsno"), Bytes.toBytes(rt_matchsno))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("trddate"), Bytes.toBytes(rt_trddate))
              table.put(theput)
              println("rt_stkbal " + rt_stkbal.toString +
                ", rt_matchsno " + rt_matchsno.toString +
                ", rt_trddate " + rt_trddate.toString)
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
