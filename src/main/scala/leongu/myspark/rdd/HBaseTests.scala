package leongu.myspark.rdd

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object HBaseTests {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SDPHBaseTests")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    toHbase(sc)
//    hbasetoConsole(sc)
    sc.stop()
  }

  def hbasetoConsole(sc: SparkContext): Unit = {
    val tablename = "t1"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2182")
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
      val age = Bytes.toString(result.getValue("cf".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
    }
  }

  def toHbase(sc: SparkContext): Unit = {
    val tablename = "t2"
    val indataRDD = sc.makeRDD(Array("rk001,leo,13", "rk002,jack,12", "rk003,kity,10", "rk004,bob,50"))

    indataRDD.foreachPartition(x => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "localhost")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2182")
      hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val htable = connection.getTable(TableName.valueOf(tablename))
      // old api
      // val htable = new HTable(hbaseConf, tablename)
      x.foreach(y => {
        val arr = y.split(",")
        val rk = arr(0)
        val name = arr(1)
        val age = arr(2)

        val put = new Put(Bytes.toBytes(rk))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(age))
        htable.put(put)
      })
    })
  }
}
