package leongu.myspark._business.points

import java.util.Date

import leongu.myspark._business.points.external.{ExTools, HBaseSink, KafkaSink}
import leongu.myspark._business.points.util.PointCons
import leongu.myspark._business.rt_asset.RTAsset.HBASE_TBL_FUND
import leongu.myspark._business.rt_asset.util.ExternalTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object Points extends Logging with PointCons {
  Logger.getLogger("org").setLevel(Level.WARN)
  var conf: mutable.Map[String, Object] = mutable.Map()
  var kafkaProducer: Broadcast[KafkaSink[String, String]] = _
  var hbaseTable: Broadcast[HBaseSink[String]] = _

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

    val spark = SparkSession
      .builder()
      //.master("local")
      .appName("realtime_asset")
      //.config("hive.metastore.uris", "thrift://localhost:9083")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 否则访问hbase失败因为对象不能序列化
      //.enableHiveSupport()
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    readConf(confFileName)

    // 1 broadcast kafka & hbase
    prepareExternal(spark.sparkContext)
    // 2 ....
    proc(spark)

    println("Over!")
  }


  def prepareExternal(sc: SparkContext) = {
    kafkaProducer = sc.broadcast(KafkaSink[String, String](conf))

    val tbl = conf.getOrElse(HBASE_TBL_FUND, "point").toString
    hbaseTable = sc.broadcast(HBaseSink[String](conf, tbl))
  }

  def proc(spark: SparkSession) = {
    import spark.sql
    val topic = conf.getOrElse(KAFKA_TOPIC_POINT, "point").toString

    // 1 cache cust base info for their phone
    sql(CUSTBASEINFO_SQL).createOrReplaceTempView(individual_cust)

    for (query <- busi_sqls) {
      logInfo(s"... business sql: $query")
      sql(query)
        .foreach(r => {
          // 1 read hbase, if cusid, point_no, date are exit then skip
          val cust_id = r.getAs[String]("cust_id")
          val busi_no = r.getAs[String]("busi_no")
          val rowkey = cust_id + "_" + busi_no

          val vals = ExternalTools.getHBaseVal(hbaseTable.value.table, rowkey, Seq("date"))
          val date = if (vals(0) == null) null else ExTools.getHBaseStringVal(vals(0))
          if (date != null && date == logDate) {
            // this data has been handled already!
            logWarning(s"row: $rowkey at $date aleady exist in hbase")
          } else {
            // 2 write kafka, in 0.11 use idempotent instead
            kafkaProducer.value.send(topic, rowkey, ExTools.jsonValue(r.getValuesMap[String](r.schema.fieldNames)).toString())
            // 3 write hbase
            hbaseTable.value.put(ExTools.hbasePut(rowkey, r.getValuesMap[String](r.schema.fieldNames)))
          }
        })
    }

    logInfo("Done!")
  }
}
