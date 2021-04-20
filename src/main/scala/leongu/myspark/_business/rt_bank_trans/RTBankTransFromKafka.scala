package leongu.myspark._business.rt_bank_trans

import java.sql.Timestamp
import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{date_format, from_json, struct}
import org.apache.spark.sql.types._
import leongu.myspark._business.util.{Cons, ExternalTools, HBaseSink, Utils}
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object RTBankTransFromKafka extends Logging with Cons {
  Logger.getLogger("org").setLevel(Level.WARN)
  var conf: mutable.Map[String, Object] = mutable.Map()

  def main(args: Array[String]) {
    val confFileName = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => null
    }

    val spark = SparkSession
      .builder()
      .appName("rt_bank_trans")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 否则访问hbase失败因为对象不能序列化
      .getOrCreate()

    logInfo("Spark Session created at " + new Date() + " ... ...")

    //    conf = Utils.readConfFromResources("/business/rt_bank_trans.yaml")
    conf = Utils.readConfFromResources(confFileName)

    // 2 realtime computing
    proc(spark)

    println("Over!")
  }

  case class Logasset(id: Long, name: String, age: Long, ts: Timestamp)

  /**
   * compute realtime bank trans
   *
   * @param spark
   */
  def proc(spark: SparkSession): Unit = {
    logInfo("Starting processing streaming data ... ...")

    import spark.implicits._

    val rawKafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getOrElse(KAFKA_SERVERS, "localhost:9092").toString)
      .option("kafka.key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      .option("kafka.value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      .option("kafka.kafka.security.authentication.sdp.publickey", conf.getOrElse(KAFKA_PUBKEY, "").toString)
      .option("kafka.kafka.security.authentication.sdp.privatekey", conf.getOrElse(KAFKA_PRIKEY, "").toString)
      .option("kafka.security.protocol", conf.getOrElse(KAFKA_PROTOCOL, "").toString)
      .option("kafka.sasl.mechanism", conf.getOrElse(KAFKA_MECHANISM, "").toString)
      .option("subscribe", conf.getOrElse(KAFKA_TOPIC, "").toString)
      .option("startingOffsets", conf.getOrElse(KAFKA_STARTING_OFFSETS, "").toString)
      .option("failOnDataLoss", "false") //参数 数据丢失，false表示工作不被禁止，会从checkpoint中获取找到断电，从断点开始从新读数据
      //      .option("max.poll.records", 10000)
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    rawKafkaDF.printSchema()
    //    create table logasset(serverid int,operdate int,cleardate int,bizdate int,sno int,relativesno int,custid bigint,custname char(16),fundid bigint,moneytype char(1),orgid char(4),brhid char(4),custkind char(1),custgroup char(1),fundkind char(1),fundlevel char(1),fundgroup char(1),digestid int,fundeffect numeric(19,2),fundbal numeric(19,2),secuid varchar(32),market char(1),biztype char(1),stkcode varchar(16),stktype char(1),bankcode char(4),trdbankcode char(4),bankbranch char(6),banknetplace char(8),stkname char(16),stkeffect bigint,stkbal bigint,orderid char(10),trdid char(1),orderqty bigint,orderprice numeric(9,3),bondintr numeric(12,8),orderdate int,ordertime int,matchqty bigint,matchamt numeric(19,2),seat char(6),matchtimes int,matchprice numeric(9,3),matchtime int,matchcode varchar(20),fee_jsxf numeric(12,2),fee_sxf numeric(12,2),fee_yhs numeric(12,2),fee_ghf numeric(12,2),fee_qsf numeric(12,2),fee_jygf numeric(12,2),fee_jsf numeric(12,2),fee_zgf numeric(12,2),fee_qtf numeric(12,2),bsflag char(2),feefront numeric(12,2),sourcetype char(1),bankid varchar(32),agentid bigint,operid int,operway char(1),operorg char(4),operlevel char(1),netaddr char(15),chkoper int,checkflag char(1),brokerid bigint,custmgrid bigint,funduser0 bigint,funduser1 int,privilege numeric(19,2),remark varchar(32),ordersno int,pathid varchar(16),cancelflag char(1),reportkind varchar(128),creditid char(1),creditflag char(1),prodcode char(12),settrate numeric(12,8),fortuneid bigint);
    val schema = StructType(List(
      StructField("serverid", IntegerType),
      StructField("operdate", IntegerType),
      StructField("cleardate", IntegerType),
      StructField("bizdate", IntegerType),
      StructField("sno", IntegerType),
      StructField("relativesno", IntegerType),
      StructField("custid", LongType),
      StructField("custname", StringType),
      StructField("fundid", LongType),
      StructField("moneytype", StringType),
      StructField("orgid", StringType),
      StructField("brhid", StringType),
      StructField("custkind", StringType),
      StructField("custgroup", StringType),
      StructField("fundkind", StringType),
      StructField("fundlevel", StringType),
      StructField("fundgroup", StringType),
      StructField("digestid", IntegerType),
      StructField("fundeffect", StringType),// DecimalType(19, 2)),
      StructField("fundbal", StringType),//DecimalType(19, 2)),
      StructField("secuid", StringType),
      StructField("market", StringType),
      StructField("biztype", StringType),
      StructField("stkcode", StringType),
      StructField("stktype", StringType),
      StructField("bankcode", StringType),
      StructField("trdbankcode", StringType),
      StructField("bankbranch", StringType),
      StructField("banknetplace", StringType),
      StructField("stkname", StringType),
      StructField("stkeffect", LongType),
      StructField("stkbal", LongType),
      StructField("orderid", StringType),
      StructField("trdid", StringType),
      StructField("orderqty", LongType),
      StructField("orderprice", StringType),//DecimalType(9, 3)),
      StructField("bondintr", StringType), //DecimalType(12, 8)),
      StructField("orderdate", IntegerType),
      StructField("ordertime", IntegerType),
      StructField("matchqty", LongType),
      StructField("matchamt", StringType),//DecimalType(19, 2)),
      StructField("seat", StringType),
      StructField("matchtimes", IntegerType),
      StructField("matchprice", StringType),//DecimalType(9, 3)),
      StructField("matchtime", IntegerType),
      StructField("matchcode", StringType),
      StructField("fee_jsxf", StringType),//DecimalType(12, 2)),
      StructField("fee_sxf", StringType),//DecimalType(12, 2)),
      StructField("fee_yhs", StringType),//DecimalType(12, 2)),
      StructField("fee_ghf", StringType),//DecimalType(12, 2)),
      StructField("fee_qsf", StringType),//DecimalType(12, 2)),
      StructField("fee_jygf", StringType),//DecimalType(12, 2)),
      StructField("fee_jsf", StringType),//DecimalType(12, 2)),
      StructField("fee_zgf", StringType),//DecimalType(12, 2)),
      StructField("fee_qtf", StringType),//DecimalType(12, 2)),
      StructField("bsflag", StringType),
      StructField("feefront", StringType),//DecimalType(12, 2)),
      StructField("sourcetype", StringType),
      StructField("bankid", StringType),
      StructField("agentid", LongType),
      StructField("operid", IntegerType),
      StructField("operway", StringType),
      StructField("operorg", StringType),
      StructField("operlevel", StringType),
      StructField("netaddr", StringType),
      StructField("chkoper", IntegerType),
      StructField("checkflag", StringType),
      StructField("brokerid", LongType),
      StructField("custmgrid", LongType),
      StructField("funduser0", LongType),
      StructField("funduser1", IntegerType),
      StructField("privilege", StringType),//DecimalType(19, 2)),
      StructField("remark", StringType),
      StructField("ordersno", IntegerType),
      StructField("pathid", StringType),
      StructField("cancelflag", StringType),
      StructField("reportkind", StringType),
      StructField("creditid", StringType),
      StructField("creditflag", StringType),
      StructField("prodcode", StringType),
      StructField("settrate", StringType),//DecimalType(12, 8)),
      StructField("fortuneid", LongType),
      StructField("collecttime", StringType)
    ))
    val logassetDF = rawKafkaDF.select(from_json($"value".cast(StringType), schema) as "json_val")
      .select($"json_val.serverid",
        $"json_val.operdate",
        $"json_val.cleardate",
        $"json_val.bizdate",
        $"json_val.sno",
        $"json_val.relativesno",
        $"json_val.custid",
        $"json_val.custname",
        $"json_val.fundid",
        $"json_val.moneytype",
        $"json_val.orgid",
        $"json_val.brhid",
        $"json_val.custkind",
        $"json_val.custgroup",
        $"json_val.fundkind",
        $"json_val.fundlevel",
        $"json_val.fundgroup",
        $"json_val.digestid",
        $"json_val.fundeffect",
        $"json_val.fundbal",
        $"json_val.secuid",
        $"json_val.market",
        $"json_val.biztype",
        $"json_val.stkcode",
        $"json_val.stktype",
        $"json_val.bankcode",
        $"json_val.trdbankcode",
        $"json_val.bankbranch",
        $"json_val.banknetplace",
        $"json_val.stkname",
        $"json_val.stkeffect",
        $"json_val.stkbal",
        $"json_val.orderid",
        $"json_val.trdid",
        $"json_val.orderqty",
        $"json_val.orderprice",
        $"json_val.bondintr",
        $"json_val.orderdate",
        $"json_val.ordertime",
        $"json_val.matchqty",
        $"json_val.matchamt",
        $"json_val.seat",
        $"json_val.matchtimes",
        $"json_val.matchprice",
        $"json_val.matchtime",
        $"json_val.matchcode",
        $"json_val.fee_jsxf",
        $"json_val.fee_sxf",
        $"json_val.fee_yhs",
        $"json_val.fee_ghf",
        $"json_val.fee_qsf",
        $"json_val.fee_jygf",
        $"json_val.fee_jsf",
        $"json_val.fee_zgf",
        $"json_val.fee_qtf",
        $"json_val.bsflag",
        $"json_val.feefront",
        $"json_val.sourcetype",
        $"json_val.bankid",
        $"json_val.agentid",
        $"json_val.operid",
        $"json_val.operway",
        $"json_val.operorg",
        $"json_val.operlevel",
        $"json_val.netaddr",
        $"json_val.chkoper",
        $"json_val.checkflag",
        $"json_val.brokerid",
        $"json_val.custmgrid",
        $"json_val.funduser0",
        $"json_val.funduser1",
        $"json_val.privilege",
        $"json_val.remark",
        $"json_val.ordersno",
        $"json_val.pathid",
        $"json_val.cancelflag",
        $"json_val.reportkind",
        $"json_val.creditid",
        $"json_val.creditflag",
        $"json_val.prodcode",
        $"json_val.settrate",
        $"json_val.fortuneid",
        $"json_val.collecttime")
      .toDF()
    logassetDF.printSchema()
    logassetDF.createOrReplaceTempView("tmp_logasset")

    val tbl = conf.getOrElse(HBASE_TBL, "").toString
    val ns = conf.getOrElse(HBASE_NAMESPACE, "default").toString
    val hbaseTable: Broadcast[HBaseSink[String]] = spark.sparkContext.broadcast(HBaseSink[String](conf, ns+":"+tbl))
    logInfo(s"------------$ns:$tbl")

    import spark.sql

    val query = conf.getOrElse(QUERY, "").toString
    logInfo(s"... business sql: $query")
    val bankTransDf = sql(query)
    logInfo(s"... business sql Over: $query")
    //    function: (Dataset[T], Long)
    bankTransDf.writeStream
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        // logInfo("--------- batch id : " + batchId)
        batchDF.rdd.foreachPartition(rows => {
          val puts = new java.util.ArrayList[Put]
          rows.foreach(
            r => {
              // 1 read hbase, if cusid, point_no, date are exit then skip
              val reverse_cptl_acid = r.getAs[String]("reverse_cptl_acid")
              val operdate = r.getAs[String]("operdate")
              val etl_dt = r.getAs[String]("etl_dt")
              val sno = r.getAs[String]("sno")
              val rowkey = reverse_cptl_acid + "_" + operdate + "_" + etl_dt + "_" + sno

              // println(s"=== $rowkey $r  ")
              // 2 write hbase
              //              hbaseTable.value.put(ExternalTools.hbasePut(rowkey, r.getValuesMap[String](r.schema.fieldNames).filter(_._2 != null)))
              puts.add(ExternalTools.hbasePut(rowkey, r.getValuesMap[String](r.schema.fieldNames).filter(_._2 != null)))
            })
          hbaseTable.value.put(puts)
        })
      })
      .option("checkpointLocation", conf.getOrElse(CHECKPOINT_LOCATION, "checkpoints/rt_bank_trans").toString)
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()
      .awaitTermination()
  }

}
