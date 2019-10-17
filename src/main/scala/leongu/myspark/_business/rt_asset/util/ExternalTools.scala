package leongu.myspark._business.rt_asset.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable

object ExternalTools extends RTACons {
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
}
