package leongu.myspark._business.rt_asset.adjusting

import leongu.myspark._business.rt_asset.util.{ExternalTools, RTACons}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait Adjusting extends Logging with RTACons {
  val adjusting_map = Map(
    1 -> ("centrd.stkasset", "bizdate"),
    2 -> ("centrd.fundasset", "busi_date"))

  /**
    * choose a day's asset as rt_asset's init state.
    */
  def adjusting(spark: SparkSession, conf: mutable.Map[String, Object], day: String): Unit = {
    logInfo("Adjusting beginning ... ...")
    for ((k, v) <- adjusting_map) {
      logInfo(s"Adjusting ${v} at $day beginning ... ...")
      cpFromHiveToHBase(spark, conf, day, k, v._1, v._2)
      logInfo(s"Adjusting ${v} at $day finished ... ...")
    }
    logInfo("Adjusting finished ... ...")
  }

  def cpFromHiveToHBase(spark: SparkSession, conf: mutable.Map[String, Object], day: String,
                        idx: Int, tbl: String, dateCol: String): Unit = {
    val bulkload_dir = conf.getOrElse(BULKLOAD_DIR, "hdfs://localhost:9000/tmp/rtasset_bulkload").toString
    var hbase = ExternalTools.getHBase(conf, conf.getOrElse(HBASE_TBL_STK, "rt_stkasset").toString)
    val hbaseConf = hbase._1
    val conn = hbase._2
    val table: Table = hbase._3

    import spark.sql

    val query = s"SELECT * FROM $tbl WHERE $dateCol = $day"
    val asset: DataFrame = sql(query)
    // asset.show()
    val row_rdd: RDD[(ImmutableBytesWritable, Seq[KeyValue])] =
      asset.rdd.map(row => transRow(row, idx))
    val hfile_rdd: RDD[(ImmutableBytesWritable, KeyValue)] = row_rdd.flatMapValues(_.iterator)
    ExternalTools.deleteHdfsPath(bulkload_dir) // rm old bulkload_dir in case dirty data
    hfile_rdd.sortBy(x => (x._1, x._2.getKeyString), true)
      .saveAsNewAPIHadoopFile(bulkload_dir,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseConf)

    // bulkload
    val load = new LoadIncrementalHFiles(hbaseConf)
    try {
      val job = Job.getInstance(hbaseConf)
      job.setJobName("Adjusting_".concat(tbl).concat(conf.getOrElse(ADJUSTING_DAY, "20190902").toString))
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)
      val start = System.currentTimeMillis()
      load.doBulkLoad(new Path(bulkload_dir), table.asInstanceOf[HTable])
      val end = System.currentTimeMillis()
      logInfo("Adjusting cost：" + (end - start) + " ms!")
    } finally {
      table.close()
      conn.close()
    }
  }

  def transRow(row: Row, idx: Int): (ImmutableBytesWritable, Seq[KeyValue]) = {
    idx match {
      case 1 => transRow4STK(row)
      case 2 => transRow4FUND(row)
    }
  }

  /**
    * STK Rowkey : fundid,secuid,orgid,market,stkcode,serverid
    *
    * @param row
    * @return
    */
  def transRow4STK(row: Row): (ImmutableBytesWritable, Seq[KeyValue]) = {
    var kvSeq: ListBuffer[KeyValue] = ListBuffer()
    val rowKey = row.getAs[Long]("fundid").toString.concat("_").
      concat(row.getAs[String]("secuid")).concat("_").
      concat(row.getAs[String]("orgid")).concat("_").
      concat(row.getAs[String]("market")).concat("_").
      concat(row.getAs[String]("stkcode")).concat("_").
      concat(row.getAs[Long]("serverid").toString)
    val wkey = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
    for ((fName, fTypeIdx) <- stk_asset_schema) {
      if (row.getAs(fName) != null) {
        val kv = genKV(row, rowKey, fName, fTypeIdx)
        kvSeq += kv
      }
    }
    (wkey, kvSeq)
  }

  /**
    * FUND Rowkey: fundid,orgid,moneytype,serverid
    *
    * @param row
    * @return
    */
  def transRow4FUND(row: Row): (ImmutableBytesWritable, Seq[KeyValue]) = {
    var kvSeq: ListBuffer[KeyValue] = ListBuffer()
    val rowKey = row.getAs[Long]("fundid").toString.concat("_").
      concat(row.getAs[String]("orgid")).concat("_").
      concat(row.getAs[String]("moneytype")).concat("_").
      concat(row.getAs[Long]("serverid").toString)
    val wkey = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
    for ((fName, fTypeIdx) <- fund_asset_schema) {
      if (row.getAs(fName) != null) {
        val kv = genKV(row, rowKey, fName, fTypeIdx)
        kvSeq += kv
      }
    }
    (wkey, kvSeq)
  }

  def genKV(row: Row, rowKey: String, name: String, typeIdx: Int): KeyValue = {
    typeIdx match {
      case 1 => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[Long](name)))
      case 2 => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[String](name)))
      case 3 => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[java.math.BigDecimal](name)))
      case _ => {
        logWarning(s"Unsupported type index $typeIdx")
        null
      }
    }
  }
}
