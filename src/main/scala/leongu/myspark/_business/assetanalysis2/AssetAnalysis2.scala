package leongu.myspark._business.assetanalysis2

import java.util.Date

import leongu.myspark._business.util.{Cons, Hive2HBase, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object AssetAnalysis2 extends Logging with Cons {
  val ASSET_ANALYSIS_OPTIMIZE = "asset_analysis_optimize"
  // index -> (hive table name, hive filter, rowkey cols, hbase table name)
  val sync_list = List(
    // for 55
    ("temp_ads.rt_cust_pl_trdy", "", Seq("trd_date"), "assetanalysis:rt_cust_pl_trdy"),
    ("temp_ads.rt_cust_pl_payf_rate_trnd", "busi_date", Seq("cptl_id", "busi_date"), "assetanalysis:rt_cust_pl_payf_rate_trnd"),
    ("temp_ads.rt_cust_pl_payf_stat", "busi_date", Seq("cptl_id", "busi_date"), "assetanalysis:rt_cust_pl_payf_stat"),
    ("temp_ads.rt_cust_pl_mkt_indx_info", "busi_date", Seq("busi_date"), "assetanalysis:rt_cust_pl_mkt_indx_info"),
    ("temp_ads.rt_cust_pl_payf_rate_stat", "", Seq("cptl_id", "stat_way"), "assetanalysis:rt_cust_pl_payf_rate_stat")
    // for xianwang
//      ("temp_ads.rt_cust_pl_trdy", "", Seq("trd_date"), "assetanalysis:rt_cust_pl_trdy"),
//    ("temp_ads.rt_cust_pl_payf_rate_trnd", "busi_date", Seq("cptl_id", "busi_date"), "assetanalysis:rt_cust_pl_payf_rate_trnd"),
//    ("temp_ads.rt_cust_pl_payf_stat", "busi_date", Seq("cptl_id", "busi_date"), "assetanalysis:rt_cust_pl_payf_stat"),
//    ("temp_ads.rt_cust_pl_mkt_indx_info", "busi_date", Seq("busi_date"), "assetanalysis:rt_cust_pl_mkt_indx_info"),
//    ("temp_ads.rt_cust_pl_payf_rate_stat", "", Seq("cptl_id", "stat_way"), "assetanalysis:rt_cust_pl_payf_rate_stat")
  )

  def main(args: Array[String]) {
    val confFileName = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => null
    }
    val conf = Utils.readConf(confFileName)
    val jobs = conf.getOrElse(ASSET_ANALYSIS_OPTIMIZE+"_job", "").toString
    val jobdate = Utils.jobDateFn(conf, SYNC_DAY, DF1)

    val spark = SparkSession
      .builder()
      //      .master("local")
      .appName(s"asset_analysis_optimize_$jobs")
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
