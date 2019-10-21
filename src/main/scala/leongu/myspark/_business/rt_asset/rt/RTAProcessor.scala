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

  case class LogAsset(
                       serverid: Long, operdate: Long, cleardate: Long, bizdate: Long, sno: Long,
                       relativesno: Long, custid: Long, custname: String, fundid: Long, moneytype: String,
                       orgid: String, brhid: String, custkind: String, custgroup: String, fundkind: String,
                       fundlevel: String, fundgroup: String, digestid: Long, fundeffect: BigDecimal, fundbal: BigDecimal,
                       secuid: String, market: String, biztype: String, stkcode: String, stktype: String,
                       bankcode: String, trdbankcode: String, bankbranch: String, banknetplace: String, stkname: String,
                       stkeffect: Long, stkbal: Long, orderid: String, trdid: String, orderqty: Long,
                       orderprice: BigDecimal, bondintr: BigDecimal, orderdate: Long, ordertime: Long, matchqty: Long,
                       matchamt: BigDecimal, seat: String, matchtimes: Long, matchprice: BigDecimal, matchtime: Long,
                       matchcode: String, fee_jsxf: BigDecimal, fee_sxf: BigDecimal, fee_yhs: BigDecimal, fee_ghf: BigDecimal,
                       fee_qsf: BigDecimal, fee_jygf: BigDecimal, fee_jsf: BigDecimal, fee_zgf: BigDecimal, fee_qtf: BigDecimal,
                       bsflag: String, feefront: BigDecimal, sourcetype: String, bankid: String, agentid: Long,
                       operid: Long, operway: String, operorg: String, operlevel: String, netaddr: String,
                       chkoper: Long, checkflag: String, brokerid: Long, custmgrid: Long, funduser0: Long,
                       funduser1: Long, privilege: BigDecimal, remark: String, ordersno: Long, pathid: String,
                       cancelflag: String, reportkind: String, creditid: String, creditflag: String, creditmethod: String,
                       rfscontractno: Long, busi_date: String
                     )

  def df_logasset(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val recordRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    recordRDD
      .map(_.split(","))
      .map(arr => LogAsset(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3).toLong, arr(4).toLong,
        arr(5).toLong, arr(6).toLong, arr(7), arr(8).toLong, arr(9),
        arr(10), arr(11), arr(12), arr(13), arr(14),
        arr(15), arr(16), arr(17).toLong, BigDecimal(arr(18)), BigDecimal(arr(19)),
        arr(20), arr(21), arr(22), arr(23), arr(24),
        arr(25), arr(26), arr(27), arr(28), arr(29),
        arr(30).toLong, arr(31).toLong, arr(32), arr(33), arr(34).toLong,
        BigDecimal(arr(35)), BigDecimal(arr(36)), arr(37).toLong, arr(38).toLong, arr(39).toLong,
        BigDecimal(arr(40)), arr(41), arr(42).toLong, BigDecimal(arr(43)), arr(44).toLong,
        arr(45), BigDecimal(arr(46)), BigDecimal(arr(47)), BigDecimal(arr(48)), BigDecimal(arr(49)),
        BigDecimal(arr(50)), BigDecimal(arr(51)), BigDecimal(arr(52)), BigDecimal(arr(53)), BigDecimal(arr(54)),
        arr(55), BigDecimal(arr(56)), arr(57), arr(58), arr(59).toLong,
        arr(60).toLong, arr(61), arr(62), arr(63), arr(64),
        arr(65).toLong, arr(66), arr(67).toLong, arr(68).toLong, arr(69).toLong,
        arr(70).toLong, BigDecimal(arr(71)), arr(72), arr(73).toLong, arr(74),
        arr(75), arr(76), arr(77), arr(78), arr(79),
        arr(80).toLong, arr(81)
      )
      ).toDF()
  }

  def df_match(spark: SparkSession, df: DataFrame): DataFrame = {
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
          val hbase = ExternalTools.getHBase(conf, conf.getOrElse(HBASE_TBL_STK, "rt_stkasset").toString)
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

  def start_fund_proc(spark: SparkSession, matchDf: DataFrame, logassetDf: DataFrame): StreamingQuery = {
    val day = conf.getOrElse(ADJUSTING_DAY, "20190902").toString
    matchDf.printSchema()
    logassetDf.printSchema()
    matchDf //      .withWatermark("ts", "300 seconds")
      .dropDuplicates()
      .where(s"trddate > $day") // if we lost checkpoints, we consume kafka from the earlist
    // 1 fund asset from match
    // fundid,orgid,moneytype,serverid
    val df1 = matchDf.select("fundid", "orgid", "moneytype", "serverid",
      "clearamt as f_change", "matchsno as f_sno", "trddate as f_date")

    // 2 fund asset from logasset
    // fundid,orgid,moneytype,serverid
    val df2 = matchDf.select("fundid", "orgid", "moneytype", "serverid",
      "fundeffect as f_change", "sno as f_sno", "bizdate as f_date")

    val union = df1.union(df2)
    union.writeStream
      .queryName("fund_log_compute")
      .outputMode("append")
      .foreachBatch { (df: DataFrame, bid: Long) =>
        df.foreachPartition(records => {
          //          println(s"create hbase client ------------------- start, bid = $bid")
          val hbase = ExternalTools.getHBase(conf, conf.getOrElse(HBASE_TBL_FUND, "rt_fundasset").toString)
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
            val vals = ExternalTools.getHBaseVal(table, rk, Seq("fundbal", "bid"))
            if (vals(1) != null && ExternalTools.getHBaseLongVal(vals(1)) == bid) {
              // for Exactly Once Semantics
              logWarning(s"! Skipping repeated record, this might happen in a crush recovery, bid: $bid record: $record")
            } else {
              // 2 compute by rules
              var rt_fundbal = BigDecimal(0.0)
              if (vals(0) == null) {
                rt_fundbal = RTARules.fund_compute(BigDecimal(0), record.getDecimal(4))
              } else {
                rt_fundbal = RTARules.fund_compute(ExternalTools.getHBaseDecimalVal(vals(0)), record.getDecimal(4))
              }
              val rt_sno = record.getLong(5)
              val rt_date = record.getLong(6)
              // 3 write to hbase
              val theput = new Put(Bytes.toBytes(rk))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("fundbal"), Bytes.toBytes(rt_fundbal.bigDecimal))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("sno"), Bytes.toBytes(rt_sno))
              theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes("bizdate"), Bytes.toBytes(rt_date))
              table.put(theput)
              println("rt_fundbal " + rt_fundbal.toString +
                ", rt_sno " + rt_sno.toString +
                ", rt_date " + rt_date.toString)
            }
          })
          //          println("close hbase connection -----------------")
          connection.close()
        })
      }
      .option("checkpointLocation", "/tmp/checkpoints/rtasset/fund")
      .start()
  }

}
