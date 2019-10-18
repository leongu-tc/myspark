package leongu.myspark._business.rt_asset.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader

import scala.collection.mutable

object ExternalTools extends RTACons with Logging {
  def getkafkaStreamReader(conf: mutable.Map[String, Object], spark: SparkSession, topic: String): DataStreamReader = {
    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getOrElse(KAFKA_SERVERS, "localhost:9092").toString)
      .option("kafka.kafka.security.authentication.sdp.publickey", conf.getOrElse(KAFKA_PUBKEY, "").toString)
      .option("kafka.kafka.security.authentication.sdp.privatekey", conf.getOrElse(KAFKA_PRIKEY, "").toString)
      .option("failOnDataLoss", "false") //参数 数据丢失，false表示工作不被禁止，会从checkpoint中获取找到断电，从断点开始从新读数据
      .option("max.poll.records", 10000)
      .option("subscribe", topic)
    if (conf.getOrElse("kafka_pri", "").toString.length > 0) {
      reader.option("kafka.security.protocol", conf.getOrElse(KAFKA_PROTOCOL, "").toString)
      reader.option("kafka.sasl.mechanism", conf.getOrElse(KAFKA_MECHANISM, "").toString)
    }
    reader
  }

  def getHBase(conf: mutable.Map[String, Object]): (Configuration, Connection, Table) = {
    var hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", conf.getOrElse(HBASE_QUORUM, "localhost").toString)
    hbaseConf.set("hbase.zookeeper.property.clientPort", conf.getOrElse(HBASE_ZK_PORT, "2182").toString)
    hbaseConf.set("zookeeper.znode.parent", conf.getOrElse(HBASE_ZK_PARENT, "/hbase").toString)
    hbaseConf.set("hbase.security.authentication.sdp.publickey", conf.getOrElse(HBASE_PUBKEY, "").toString)
    hbaseConf.set("hbase.security.authentication.sdp.privatekey", conf.getOrElse(HBASE_PRIKEY, "").toString)
    hbaseConf.set("hbase.security.authentication.sdp.username", conf.getOrElse(HBASE_USER, "hbase").toString)
    var connection = ConnectionFactory.createConnection(hbaseConf)
    var table = connection.getTable(TableName.valueOf(conf.getOrElse(HBASE_RESULT_TBL, "t1").toString))
    (hbaseConf, connection, table)
  }

  def getHBaseVal(t: Table, rk: String, cols: Seq[String]): Seq[Array[Byte]] = {
    val get = new Get(Bytes.toBytes(rk))
    for (col <- cols) yield t.get(get).getColumnLatestCell(HBASE_CF_BYTES, Bytes.toBytes(col)).getValueArray
  }

  def deleteHdfsPath(url: String) = {
    logInfo(s"Delete HDFS Path: $url")
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path: Path = new Path(url)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }
}
