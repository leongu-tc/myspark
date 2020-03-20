package leongu.myspark._business.util

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.util.Bytes

trait Cons {
  /** config */
  val KAFKA_SERVERS = "kafka_servers"
  val KAFKA_PUBKEY = "kafka_pub"
  val KAFKA_PRIKEY = "kafka_pri"
  val KAFKA_TOPIC_MATCH = "kafka_topic_match"
  val KAFKA_TOPIC_LOG = "kafka_topic_log"
  val KAFKA_PROTOCOL = "kafka_protocol"
  val KAFKA_MECHANISM = "kafka_mechanism"

  val HBASE_QUORUM = "hbase_quorum"
  val HBASE_ZK_PORT = "hbase_zk_port"
  val HBASE_ZK_PARENT = "hbase_zk_parent"
  val HBASE_PUBKEY = "hbase_pub"
  val HBASE_PRIKEY = "hbase_pri"
  val HBASE_USER = "hbase_user"

  val SYNC_DAY = "sync_day"
  val BULKLOAD_DIR = "bulkload_dir"

  // cons
  val HALF_DAY = 12 * 3600 * 1000
  val HBASE_CF = "cf"
  val HBASE_CF_BYTES = Bytes.toBytes(HBASE_CF)

  val pDecimal = "(decimal.*)".r

  // date format
  val DF1 = new SimpleDateFormat("yyyyMMdd")
  val DF2 = new SimpleDateFormat("yyyy-MM-dd")
  val DF3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
}
