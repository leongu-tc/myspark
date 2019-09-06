package leongu.myspark.rdd.sdp

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SDPHBaseTests {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SDPHBaseTests")
    //.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "t1"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "sdp-10-88-100-140,sdp-10-88-100-141,sdp-10-88-100-142")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)

    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach { case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes, "name".getBytes))
      val age = Bytes.toInt(result.getValue("cf".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
    }

    sc.stop()
  }
}
