package leongu.myspark._business.clearedstock

import java.util.Date

import leongu.myspark._business.util.{Cons, Hive2HBase, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object ClearedStock extends Logging with Cons {
  val CLEARED_STOCK_JOB = "cleared_stock_job"
  val sync_list = List(
    // for 55
    //    ("temp_ads.rt_cust_daily_return_rate", "busi_date", Seq("cptl_acc_id", "busi_date"),"assetanalysis:rt_cust_daily_return_rate"),
    //    ("temp_ads.rt_cust_daily_stkreturn", "busi_date", Seq("cptl_acc_id", "busi_date", "stk_cd"),"assetanalysis:rt_cust_daily_stkreturn"),
    //    ("temp_ads.rt_cust_return_data", "busi_date", Seq("cptl_acc_id", "busi_date"),"assetanalysis:rt_cust_return_data"),
    //    ("temp_ads.rt_cust_month_stk_return", "", Seq("cptl_acc_id"),"assetanalysis:rt_cust_month_stk_return"),
    //    ("temp_ads.rt_cust_stk_rank", "", Seq("cptl_acc_id", "rank", "pl_flag"),"assetanalysis:rt_cust_stk_rank")
    // for xianwang
    ("ads.rt_cust_daily_return_rate", "busi_date", Seq("cptl_acc_id", "busi_date"), "assetanalysis:rt_cust_daily_return_rate"),
    ("ads.rt_cust_daily_stkreturn", "busi_date", Seq("cptl_acc_id", "busi_date", "stk_cd"), "assetanalysis:rt_cust_daily_stkreturn"),
    ("ads.rt_cust_return_data", "busi_date", Seq("cptl_acc_id", "busi_date"), "assetanalysis:rt_cust_return_data"),
    ("ads.rt_cust_month_stk_return", "", Seq("cptl_acc_id"), "assetanalysis:rt_cust_month_stk_return"),
    ("ads.rt_cust_stk_rank", "", Seq("cptl_acc_id", "stk_cd"), "assetanalysis:rt_cust_stk_rank")
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
          case _ => println(s"Job name :$job unsupported!")
        }
      }
    )

    println("Over!")
  }
}
