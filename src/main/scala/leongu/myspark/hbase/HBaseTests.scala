package leongu.myspark.hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object HBaseTests {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      //      .master("spark://localhost:7077")
            .master("local")
      .appName("Source example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

//    toHbase(spark.sparkContext)
    hbasetoConsole(spark)

    println("done!")
  }

  def hbasetoConsole(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    val tablename = "clearedstock:rt_cust_cleared_stock"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2182")
    //      hbaseConf.set("hbase.zookeeper.quorum", "sdp-10-88-100-140,sdp-10-88-100-141,sdp-10-88-100-142")
    //      hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    //      hbaseConf.set("hbase.security.authentication.sdp.publickey", "ItC2TbwGpXPHK9lCS5cGEWI7tzH8AoAnLKtJ")
    //      hbaseConf.set("hbase.security.authentication.sdp.privatekey", "pc0mO6NCjixoMZf9FSjJeVHADP6sng9T")
    //      hbaseConf.set("hbase.security.authentication.sdp.username", "hbase")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)

    /** add row filter
      *
    val startRowkey="0,110000,20180220"
    val endRowkey="0,110000,20180302"
    //开始rowkey和结束一样代表精确查询某条数据

    //组装scan语句
    val scan=new Scan(Bytes.toBytes(startRowkey),Bytes.toBytes(endRowkey))
    scan.setCacheBlocks(false)
    // scan.addFamily(Bytes.toBytes("ks"));
    // scan.addColumn(Bytes.toBytes("ks"), Bytes.toBytes("data"))

    //将scan类转化成string类型
     val proto= ProtobufUtil.toScan(scan)
      val ScanToString = Base64.encodeBytes(proto.toByteArray());
    conf.set(TableInputFormat.SCAN,ScanToString)
      */
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)

    val structField1 = StructField("rowkey", StringType)
    val structField2 = StructField("col1", StringType)

    val schema = StructType(Seq(structField1, structField2))

    val rdd = hBaseRDD.map(x => {
      val result = x._2
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes, "col1".getBytes))
      Row.fromSeq(Seq(key, name))
    })
    val df = spark.createDataFrame(rdd, schema)
    df.show()
//    hBaseRDD.foreach { case (_, result) => {
//      //获取行键
//      val key = Bytes.toString(result.getRow)
//      //通过列族和列名获取列
//      val name = Bytes.toString(result.getValue("cf".getBytes, "col1".getBytes))
//      println("Row key:" + key + " Name:" + name)
////      val name = Bytes.toString(result.getValue("cf".getBytes, "name".getBytes))
////      val age = Bytes.toString(result.getValue("cf".getBytes, "age".getBytes))
////      println("Row key:" + key + " Name:" + name + " Age:" + age)
//    }
//    }
  }

  def toHbase(sc: SparkContext): Unit = {
    val tablename = "t2"
    val indataRDD = sc.makeRDD(Array("rk001,leo,13", "rk002,jack,12", "rk003,kity,10", "rk004,bob,50"))

    indataRDD.foreachPartition(x => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "localhost")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2182")
      //      hbaseConf.set("hbase.zookeeper.quorum", "sdp-10-88-100-140,sdp-10-88-100-141,sdp-10-88-100-142")
      //      hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
      //      hbaseConf.set("hbase.security.authentication.sdp.publickey", "ItC2TbwGpXPHK9lCS5cGEWI7tzH8AoAnLKtJ")
      //      hbaseConf.set("hbase.security.authentication.sdp.privatekey", "pc0mO6NCjixoMZf9FSjJeVHADP6sng9T")
      //      hbaseConf.set("hbase.security.authentication.sdp.username", "hbase")
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
