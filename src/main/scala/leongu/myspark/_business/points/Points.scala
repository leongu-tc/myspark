package leongu.myspark._business.points

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import leongu.myspark._business.points.external.{ExTools, HBaseSink, KafkaSink}
import leongu.myspark._business.points.util.PointCons
import leongu.myspark._business.rt_asset.util.ExternalTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object Points extends Logging with PointCons {
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

    val spark = SparkSession
      .builder()
      //.master("local")
      .appName("point")
      //.config("hive.metastore.uris", "thrift://localhost:9083")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 否则访问hbase失败因为对象不能序列化
      .enableHiveSupport()
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    readConf(confFileName)

    // 2 ....
    proc(spark)

    println("Over!")
  }

  def proc(spark: SparkSession) = {
    logInfo(s"------------broadcast kafka")
    val kafkaProducer:Broadcast[KafkaSink[String,String]]={
      val kafkaProducerConfig = {
        val p = new Properties();
        p.setProperty("bootstrap.servers",conf.getOrElse(KAFKA_SERVERS, "localhost:9092").toString)
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("kafka.security.authentication.sdp.publickey", conf.getOrElse(KAFKA_PUBKEY, "").toString)
        p.put("kafka.security.authentication.sdp.privatekey", conf.getOrElse(KAFKA_PRIKEY, "").toString)
        p.put("security.protocol", conf.getOrElse(KAFKA_PROTOCOL, "").toString)
        p.put("sasl.mechanism", conf.getOrElse(KAFKA_MECHANISM, "").toString)
        p
      }
      spark.sparkContext.broadcast(KafkaSink[String,String](kafkaProducerConfig))
    }
    val tbl = conf.getOrElse(HBASE_TBL_POINT, "point").toString
    val hbaseTable: Broadcast[HBaseSink[String]]  = spark.sparkContext.broadcast(HBaseSink[String](conf, tbl))
    logInfo(s"------------$tbl")

    import spark.sql
    val topic = conf.getOrElse(KAFKA_TOPIC_POINT, "point").toString
    val logDate = conf.getOrElse(LOG_DATE, new SimpleDateFormat("yyyyMMdd").format(yesterday.getTime)).toString
    // 1 cache cust base info for their phone
    sql(CUSTBASEINFO_SQL).createOrReplaceTempView(individual_cust)

    for (query <- busi_sqls) {
      logInfo(s"... business sql: $query")
      val res = sql(query)
//      logInfo("count: "+res.count())
      logInfo(s"... business sql Over: $query")

        res.foreach(r => {
          // 1 read hbase, if cusid, point_no, date are exit then skip
          val cust_id = r.getAs[String]("cust_id")
          val busi_no = r.getAs[String]("busi_no")
          val rowkey = cust_id + "_" + busi_no

          println(s"=== $rowkey $r  ")
          val vals = ExternalTools.getHBaseVal(hbaseTable.value.table, rowkey, Seq("busi_date"))
          val date = if (vals(0) == null) null else ExTools.getHBaseStringVal(vals(0))
          println(s"===2 $date  $logDate")
          // cannot use logDate,闭包产生的错误，这里会重新计算为 T-1，而不是我们配置的 logDate
          if (date != null && date == logDate) {
            // this data has been handled already!
            println(s"===3 row: $rowkey at $date aleady exist in hbase")
          } else {
            // 2 write kafka, in 0.11 use idempotent instead
            kafkaProducer.value.send(topic, rowkey, ExTools.jsonValue(r.getValuesMap[String](r.schema.fieldNames)).toString())
            // 3 write hbase
            hbaseTable.value.put(ExTools.hbasePut(rowkey, r.getValuesMap[String](r.schema.fieldNames)))
          }
        })

      logInfo(s"... write to hbase and kafka finished: $query")
    }

    logInfo("Done!")
  }
}
