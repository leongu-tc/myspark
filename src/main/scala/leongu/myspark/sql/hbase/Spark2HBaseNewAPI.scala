package leongu.myspark.sql.hbase

import java.text.DecimalFormat

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Spark2HBaseNewAPI {
  val tableName = "test01"
  val cf = "cf"
  val num=1000000
  val df2 = new DecimalFormat("00000000")
  def main(args: Array[String]) = {
    val sc = getSparkSession().sparkContext
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "hadoop01:2181")
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin
    val jobConf = new JobConf(hbaseConf, this.getClass)
    // 设置表名
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 如果表不存在则创建表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor(cf)
      desc.addFamily(hcd)
      admin.createTable(desc)
    }

    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
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

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    val end = System.currentTimeMillis()
    println("入库用时：" + (end - start))
    sc.stop()

  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().
      appName("SparkToHbase").
      master("local").
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      getOrCreate()
  }
}
