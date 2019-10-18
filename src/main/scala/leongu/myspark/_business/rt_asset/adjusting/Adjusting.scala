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
  /**
    * choose a day's asset as rt_asset's init state.
    */
  def adjusting(spark: SparkSession, conf: mutable.Map[String, Object], day: String): Unit = {
    logInfo("Adjusting beginning ... ...")
    cpFromHiveToHBase(spark, conf, day)
    logInfo("Adjusting finished ... ...")
  }

  def cpFromHiveToHBase(spark: SparkSession, conf: mutable.Map[String, Object], day: String): Unit = {
    var hbase = ExternalTools.getHBase(conf)
    val hbaseConf = hbase._1
    val conn = hbase._2
    val table: Table = hbase._3

    import spark.sql

    val query = s"SELECT * FROM centrd.stkasset WHERE bizdate = $day"
    val stkasset: DataFrame = sql(query)
    stkasset.show()
    val row_rdd: RDD[(ImmutableBytesWritable, Seq[KeyValue])] =
      stkasset.rdd.map(row => transRow2Log(row))
    val hfile_rdd: RDD[(ImmutableBytesWritable, KeyValue)] = row_rdd.flatMapValues(_.iterator)
    ExternalTools.deleteHdfsPath(BULKLOAD_DIR) // rm old bulkload_dir in case dirty data
    hfile_rdd.sortBy(x => (x._1, x._2.getKeyString), true)
      .saveAsNewAPIHadoopFile(BULKLOAD_DIR,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseConf)

    // bulkload
    val load = new LoadIncrementalHFiles(hbaseConf)
    try {
      val job = Job.getInstance(hbaseConf)
      job.setJobName("Adjusting_".concat(conf.getOrElse(ADJUSTING_DAY, "20190902").toString))
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)
      val start = System.currentTimeMillis()
      load.doBulkLoad(new Path(BULKLOAD_DIR), table.asInstanceOf[HTable])
      val end = System.currentTimeMillis()
      logInfo("Adjusting cost：" + (end - start) + " ms!")
    } finally {
      table.close()
      conn.close()
    }
  }

  /**
    * Rowkey : fundid,secuid,orgid,market,stkcode,serverid
    *
    * @param row
    * @return
    */
  def transRow2Log(row: Row): (ImmutableBytesWritable, Seq[KeyValue]) = {
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
