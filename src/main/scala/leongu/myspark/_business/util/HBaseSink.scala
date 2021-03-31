package leongu.myspark._business.util


import leongu.myspark._business.rt_asset.util.ExternalTools._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable

class HBaseSink[T](createTable: () => Table) extends Serializable {

  lazy val table = createTable()

  def put(put: Put) = {
    table.put(put)
  }
  def put(puts: java.util.ArrayList[Put]) = {
    table.put(puts)
  }
}

object HBaseSink {
  def apply[T](conf: mutable.Map[String, Object], tbl: String): HBaseSink[T] = {
    val createTable = () => {
      println("------------ HBaseSink -----------")
      conf.map(kv => println(kv.toString()))
      var hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", conf.getOrElse(HBASE_QUORUM, "localhost").toString)
      hbaseConf.set("hbase.zookeeper.property.clientPort", conf.getOrElse(HBASE_ZK_PORT, "2182").toString)
      hbaseConf.set("zookeeper.znode.parent", conf.getOrElse(HBASE_ZK_PARENT, "/hbase").toString)
      hbaseConf.set("hbase.security.authentication.sdp.publickey", conf.getOrElse(HBASE_PUBKEY, "").toString)
      hbaseConf.set("hbase.security.authentication.sdp.privatekey", conf.getOrElse(HBASE_PRIKEY, "").toString)
      hbaseConf.set("hbase.security.authentication.sdp.username", conf.getOrElse(HBASE_USER, "hbase").toString)
      var connection = ConnectionFactory.createConnection(hbaseConf)
      var table = connection.getTable(TableName.valueOf(tbl))
      sys.addShutdownHook {
        connection.close()
      }
      println("------------ HBaseSink --------------")
      table
    }

    new HBaseSink(createTable)
  }
}