package leongu.myspark.hbase

import java.text.DecimalFormat

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object Spark2HBaseOldApi {
  val tableName = "test01"
  val cf = "cf"
  val df2 = new DecimalFormat("00000000")
  val num = 1000000

  //10W用时2623ms、2596ms
  //100W用时14929ms、13753ms
  def main(args: Array[String]): Unit = {
    val sc = getSparkSession().sparkContext
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2182")
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin
    val jobConf = new JobConf(hbaseConf, this.getClass)
    // 设置表名
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    // 如果表不存在则创建表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor(cf)
      desc.addFamily(hcd)
      admin.createTable(desc)
    }

    var list = ListBuffer[Put]()
    println("数据准备中。。。。")
    for (i <- 0 to num) {
      val put = new Put(df2.format(i).getBytes())
      put.addColumn(cf.getBytes(), "field".getBytes(), "abc".getBytes())
      list.append(put)
    }
    println("数据准备完成！")
    val data = sc.makeRDD(list.toList).map(x => {
      (new ImmutableBytesWritable, x)
    })
    val start = System.currentTimeMillis()
    data.saveAsHadoopDataset(jobConf)
    val end = System.currentTimeMillis()
    println("入库用时：" + (end - start))
    sc.stop()
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().
      appName("SparkToHbase").
      master("local[4]").
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      getOrCreate()
  }
}
