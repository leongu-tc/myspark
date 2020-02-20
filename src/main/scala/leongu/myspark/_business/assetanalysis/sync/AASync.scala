package leongu.myspark._business.assetanalysis.sync

import leongu.myspark._business.assetanalysis.util.AACons
import leongu.myspark._business.rt_asset.util.ExternalTools
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

object AASync extends Logging with AACons {
  // index -> (hive table name, hive filter, rowkey cols, hbase table name)
  val sync_list = List(
    // for 55
//    ("temp_ads.rt_cust_daily_return_rate", "busi_date", Seq("cptl_acc_id", "busi_date"),"assetanalysis:rt_cust_daily_return_rate"),
//    ("temp_ads.rt_cust_daily_stkreturn", "busi_date", Seq("cptl_acc_id", "busi_date", "stk_cd"),"assetanalysis:rt_cust_daily_stkreturn"),
//    ("temp_ads.rt_cust_return_data", "busi_date", Seq("cptl_acc_id", "busi_date"),"assetanalysis:rt_cust_return_data"),
//    ("temp_ads.rt_cust_month_stk_return", "", Seq("cptl_acc_id"),"assetanalysis:rt_cust_month_stk_return"),
//    ("temp_ads.rt_cust_stk_rank", "", Seq("cptl_acc_id", "rank", "pl_flag"),"assetanalysis:rt_cust_stk_rank")
    // for xianwang
    ("ads.rt_cust_daily_return_rate", "busi_date", Seq("cptl_acc_id", "busi_date"),"assetanalysis:rt_cust_daily_return_rate"),
    ("ads.rt_cust_daily_stkreturn", "busi_date", Seq("cptl_acc_id", "busi_date", "stk_cd"),"assetanalysis:rt_cust_daily_stkreturn"),
    ("ads.rt_cust_return_data", "busi_date", Seq("cptl_acc_id", "busi_date"),"assetanalysis:rt_cust_return_data"),
    ("ads.rt_cust_month_stk_return", "", Seq("cptl_acc_id"),"assetanalysis:rt_cust_month_stk_return"),
    ("ads.rt_cust_stk_rank", "", Seq("cptl_acc_id", "rank", "pl_flag"),"assetanalysis:rt_cust_stk_rank")
  )

  /**
   *
   *
   */
  /**
   * -1 is for full history data
   * 20200114 is for daily data
   *
   * @param spark
   * @param conf
   * @param day
   */
  def sync(spark: SparkSession, conf: mutable.Map[String, Object], day: String): Unit = {
    logInfo("Asset Analysis sync to hbase beginning ... ...")
    for (v <- sync_list) {
      logInfo(s"Sync ${v} data at $day beginning ... ...")
      val columnMap = new mutable.HashMap[String, String]()
      val schema: DataFrame = spark.catalog.listColumns(v._1).select("name", "dataType")
      schema.show()
      schema.collect().foreach(
        row => columnMap.put(row.getAs[String]("name"), row.getAs[String]("dataType"))
      )
      println(s"===== schema: $columnMap")

      cpFromHiveToHBase(spark, conf, day, v._1, v._2, v._3, v._4, columnMap)
      logInfo(s"Sync ${v} data at $day finished ... ...")
    }
    logInfo("Asset Analysis sync to hbase finished ... ...")
  }

  /**
   *
   * @param spark
   * @param conf
   * @param day     -1 is for full history data
   * @param idx     hive table index in sync_map
   * @param tbl     table name
   * @param dateCol date column name
   */
  def cpFromHiveToHBase(spark: SparkSession, conf: mutable.Map[String, Object], day: String,
                        tbl: String, dateCol: String, rowkeyCols: Seq[String], hbaseTbl: String,
                        columnMap: mutable.HashMap[String, String]): Unit = {
    val bulkload_dir = conf.getOrElse(BULKLOAD_DIR, "hdfs://localhost:9000/tmp/rtasset_bulkload").toString
    val hbase = ExternalTools.getHBase(conf, hbaseTbl)
    val hbaseConf = hbase._1
    val conn = hbase._2
    val table: Table = hbase._3

    import spark.sql

    val query = genQuerySQL(day, tbl, dateCol)
    println(s"==== query : $query")
    val df: DataFrame = sql(query).coalesce(1000)
    df.show()
    val row_rdd: RDD[(ImmutableBytesWritable, Seq[KeyValue])] =
      df.rdd.map(row => transRow(row, day, rowkeyCols, tbl, columnMap))
    val hfile_rdd: RDD[(ImmutableBytesWritable, KeyValue)] = row_rdd.flatMapValues(_.iterator)
    ExternalTools.deleteHdfsPath(bulkload_dir) // rm old bulkload_dir in case dirty data
    hfile_rdd.sortBy(x => (x._1, x._2.getKeyString), true)
      .coalesce(32)
      .saveAsNewAPIHadoopFile(bulkload_dir,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseConf)

    // bulkload
    val load = new LoadIncrementalHFiles(hbaseConf)
    try {
      val job = Job.getInstance(hbaseConf)
      job.setJobName("AssetAnalysis_sync_".concat(tbl).concat(day))
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)
      println(s"==== bulkload : $tbl")
      val start = System.currentTimeMillis()
      load.doBulkLoad(new Path(bulkload_dir), table.asInstanceOf[HTable])
      val end = System.currentTimeMillis()
      logInfo("AssetAnalysis sync cost：" + (end - start) + " ms!")
    } finally {
      table.close()
      conn.close()
    }
  }

  def transRow(row: Row, day: String, rowkeyCols: Seq[String], tbl: String
               , columnMap: mutable.HashMap[String, String]): (ImmutableBytesWritable, Seq[KeyValue]) = {
    var kvSeq: ListBuffer[KeyValue] = ListBuffer()
    val rowKey = genRowkey(row, columnMap, rowkeyCols)
    val wkey = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
    for ((cName, cType) <- columnMap) {
      if (row.getAs(cName) != null) {
        val kv = genKV(row, rowKey, cName, cType)
        kvSeq += kv
      }
    }
    (wkey, kvSeq)
  }

  def genRowkey(row: Row, columnMap: mutable.HashMap[String, String], rowkeyCols: Seq[String]): String = {
    val vals =
      for (col <- rowkeyCols) yield {
        getStringVal(row, col, columnMap.getOrElse(col, "string"))
      }
    vals.reduceLeft(_ + "_" + _)
  }

  def getStringVal(row: Row, col: String, typeStr: String): String = {
    if (col != null && col.length > 0) {
      typeStr match {
        case "int" => row.getAs[Int](col).toString
        case "long" => row.getAs[Long](col).toString
        case "float" => row.getAs[Float](col).toString
        case "double" => row.getAs[Double](col).toString
        case "string" => row.getAs[String](col)
        case pDecimal(typeStr) => row.getAs[BigDecimal](col).toString
        case _ => {
          logWarning(s"Unsupported type index $typeStr")
          ""
        }
      }
    } else {
      ""
    }
  }

  def genKV(row: Row, rowKey: String, name: String, typeStr: String): KeyValue = {
    typeStr match {
      // TODO add other datatype, all to string
      case "int" => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[Int](name).toString))
      case "long" => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[Long](name).toString))
      case "float" => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[Float](name).toString))
      case "double" => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[Double](name).toString))
      case "string" => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[String](name).toString))
      case pDecimal(typeStr) => new KeyValue(Bytes.toBytes(rowKey), HBASE_CF_BYTES,
        Bytes.toBytes(name), Bytes.toBytes(row.getAs[java.math.BigDecimal](name).toPlainString))
      case _ => {
        logWarning(s"Unsupported type index $typeStr")
        null
      }
    }
  }

  def genQuerySQL(day: String, tbl: String, dateCol: String): String = {
//    day match {
//      case "-1" => s"SELECT * FROM $tbl WHERE cptl_acc_id IN ('10000000001', '60000000251', '60080000251')" // full history
//      case _ => {
//        dateCol match {
//          case "" => s"SELECT * FROM $tbl" // full history
//          case _ => s"SELECT * FROM $tbl WHERE cptl_acc_id IN ('10000000001', '60000000251', '60080000251') AND $dateCol = $day" // daily data
//        }
//      }
//    }
    day match {
      case "-1" => s"SELECT * FROM $tbl" // full history
      case _ => {
        dateCol match {
          case "" => s"SELECT * FROM $tbl" // full history
          case _ => s"SELECT * FROM $tbl WHERE $dateCol = $day" // daily data
        }
      }
    }
  }
}
