package leongu.myspark._business.cchr


import java.util.Date

import leongu.myspark._business.util.{Cons, FeedInTime, Hive2HBase, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object CCHR extends Logging with Cons {
  val CCHR = "cchr"
  // index -> (hive table name, hive filter, rowkey cols, hbase table name)
  val sync_list = List(
    ("cchr.c11", "", Seq("a0188"), "cchr:c11")
  )

  def main(args: Array[String]) {
    val confFileName = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => null
    }
    val conf = Utils.readConf(confFileName)
    val jobs = conf.getOrElse(CCHR + "_job", "").toString
    val jobdate = Utils.jobDateFn(conf, SYNC_DAY, DF1)

    val spark = SparkSession
      .builder()
      //      .master("local")
      .appName(s"CCHR_$jobs")
      //      .config("hive.metastore.uris", "thrift://localhost:9083") // for ide TEST
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 否则访问hbase失败因为对象不能序列化
      .config("spark.hive.mapred.supports.subdirectories", "true") // for HIVE CTAS UNION ALL table's subdirectories
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      .enableHiveSupport()
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    // 2 jobs
    jobs.split(",").map(
      job => {
        logInfo(s"----- Task name: $job")
        job match {
          case "sync" => {
            Hive2HBase.sync(spark, conf, sync_list, jobdate)
          }
          case "feed_in" => {
            // 将表更新情况记录到 hbase 的 sdp:feed_in_time, 日期必须是 yyyyMMdd 格式
            FeedInTime.feedIn(spark, conf, sync_list, Utils.jobDateFn(conf, SYNC_DAY, DF1))
          }
          case _ => println(s"Job name :$job unsupported!")
        }
      }
    )

    println("Over!")
  }
}