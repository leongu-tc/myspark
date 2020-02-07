package leongu.myspark._business.assetanalysis

import java.util.{Date, Properties}

import leongu.myspark._business.assetanalysis.sync.AASync
import leongu.myspark._business.assetanalysis.util.AACons
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object AssetAnalysis extends Logging with AACons {
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

  def main(args: Array[String]) {
    val confFileName = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => null
    }
    readConf(confFileName)
    val jobs = conf.getOrElse(ASSET_ANALYSIS_JOB, "").toString

    val spark = SparkSession
      .builder()
//      .master("local")
      .appName(s"assset_analysis_$jobs")
//      .config("hive.metastore.uris", "thrift://localhost:9083") // for ide TEST
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 否则访问hbase失败因为对象不能序列化
      .config("spark.hive.mapred.supports.subdirectories","true") // for HIVE CTAS UNION ALL table's subdirectories
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
      .enableHiveSupport()
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    // 2 jobs
    jobs.split(",").map(
      job => {
        job match {
          case "sync" => {
            println(s"----- Task name: $job")
            if (conf.contains(SYNC_DAY)) {
              AASync.sync(spark, conf, conf.getOrElse(SYNC_DAY, "-1").toString)
            } else {
              // for azkaban env 20200114
              var dataTime = System.getenv("SDP_DATA_TIME")
              println(s"------ $dataTime")
              val dataTime2 = dataTime.substring(0,4).concat(dataTime.substring(4,6)).concat(dataTime.substring(6,8))
              AASync.sync(spark, conf, dataTime2)
            }
          }
          case _ => println(s"Job name :$job unsupported!")
        }
      }
    )

    println("Over!")
  }
}
