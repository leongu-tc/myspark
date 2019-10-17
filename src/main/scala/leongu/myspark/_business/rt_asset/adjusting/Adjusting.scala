package leongu.myspark._business.rt_asset.adjusting

import leongu.myspark._business.rt_asset.util.RTACons
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.TreeMap
import scala.collection.mutable

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
    var hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", conf.getOrElse("hbase_quorum", "localhost").toString)
    hbaseConf.set("hbase.zookeeper.property.clientPort", conf.getOrElse("hbase_zk_port", "2182").toString)
    hbaseConf.set("zookeeper.znode.parent", conf.getOrElse("hbase_zk_parent", "/hbase").toString)
    hbaseConf.set("hbase.security.authentication.sdp.publickey", conf.getOrElse("hbase_pub", "").toString)
    hbaseConf.set("hbase.security.authentication.sdp.privatekey", conf.getOrElse("hbase_pri", "").toString)
    hbaseConf.set("hbase.security.authentication.sdp.username", conf.getOrElse("hbase_user", "hbase").toString)

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table: Table = conn.getTable(TableName.valueOf(HBASE_RESULT_TBL))

    import spark.implicits._
    import spark.sql

    val query = s"SELECT * FROM centrd.stkasset WHERE bizdate = $day"
    val stkasset: DataFrame = sql(query)

    val rdd = stkasset.flatMap(row => transRow2Log(row).iterator).coalesce(1).rdd.sortByKey()
      .saveAsNewAPIHadoopFile(BULKLOAD_DIR,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseConf)


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
  def transRow2Log(row: Row): TreeMap[ImmutableBytesWritable, KeyValue] = {
    val rowKey = row.getAs[String]("fundid").concat("_").
      concat(row.getAs[String]("secuid")).concat("_").
      concat(row.getAs[String]("orgid")).concat("_").
      concat(row.getAs[String]("market")).concat("_").
      concat(row.getAs[String]("stkcode")).concat("_").
      concat(row.getAs[String]("serverid"))
    val wkey = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
    for ((fName, fTypeIdx) <- stk_asset_schema) yield (wkey, genKV(row, rowKey, fName, fTypeIdx))
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
