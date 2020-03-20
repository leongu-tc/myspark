package leongu.myspark._business.util

import java.util.Date

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object FeedInTime extends Logging with Cons {
  val tbl = "sdp:feed_in_time"
  val NAME = "name"
  val TYPE = "type"
  val FEED_IN_TIME = "feed_in_time"
  val LAST_DATA_TIME = "last_data_time"
  val LATEST_DATA_TIME = "latest_data_time"

  /**
   *
   *
   */
  /**
   * -1 is for full history data
   * 20200114 is for daily data
   *
   * @param spark
   * @param conf
   * @param day yyyyMMdd or -1 means full history
   */
  def feedIn(spark: SparkSession, conf: mutable.Map[String, Object],
             sync_list: List[(String, String, Seq[String], String)],
             day: String): Unit = {
    logInfo("feed in start ... ...")
    val hbaseTable: Broadcast[HBaseSink[String]] = spark.sparkContext.broadcast(HBaseSink[String](conf, tbl))
    val feedInTime = DF3.format(new Date(System.currentTimeMillis()))
    val lastDataTime = day
    val typeVal = "hbase"
    for (v <- sync_list) {
      val name = v._4
      val rowkey = name + "_hbase"
      val vals = ExternalTools.getHBaseVal(hbaseTable.value.table, rowkey, Seq(LATEST_DATA_TIME))
      var latestDataTime = if (vals(0) == null) -1 else ExternalTools.getHBaseStringVal(vals(0)).toInt
      latestDataTime = Math.max(latestDataTime, day.toInt)
      logInfo(s"feed in $name latest data time: $latestDataTime")
      val map = Map(
        NAME -> name,
        TYPE -> typeVal,
        FEED_IN_TIME -> feedInTime,
        LAST_DATA_TIME -> lastDataTime,
        LATEST_DATA_TIME -> latestDataTime.toString)
      hbaseTable.value.put(ExternalTools.hbasePut(rowkey, map))
    }
    logInfo("feed in end ... ...")
  }
}
