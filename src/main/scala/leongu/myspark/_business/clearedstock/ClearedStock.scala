package leongu.myspark._business.clearedstock

import java.util.Date

import leongu.myspark._business.util.{Cons, FeedInTime, Hive2HBase, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ClearedStock extends Logging with Cons {
  val CLEARED_STOCK_JOB = "cleared_stock_job"
  // index -> (hive table name, hive filter, rowkey cols, hbase table name)
  val sync_list = List(
    // for 55
    // ("temp_ads.rt_cust_cleared_stock", "clear_date", Seq("fund_id","clear_date","stk_cd","market"), "clearedstock:rt_cust_cleared_stock"),
    // ("temp_ads.rt_cust_cleared_stock_detail", "clear_date", Seq("fund_id","stk_cd","market","clear_date","trd_date","trd_sno"), "clearedstock:rt_cust_cleared_stock_detail")
    // for xianwang
    ("ads.rt_cust_cleared_stock", "clear_date", Seq("fund_id","clear_date","stk_cd","market"), "clearedstock:rt_cust_cleared_stock"),
    ("ads.rt_cust_cleared_stock_detail", "clear_date", Seq("fund_id","stk_cd","market","clear_date","trd_date","trd_sno"), "clearedstock:rt_cust_cleared_stock_detail")
  )

  def main(args: Array[String]) {
    val confFileName = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => null
    }
    val conf = Utils.readConf(confFileName)
    val jobs = conf.getOrElse(CLEARED_STOCK_JOB, "").toString
    val jobdate = Utils.jobDateFn(conf, SYNC_DAY, DF1)

    val spark = SparkSession
      .builder()
      //      .master("local")
      .appName(s"cleared_stock_$jobs")
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
