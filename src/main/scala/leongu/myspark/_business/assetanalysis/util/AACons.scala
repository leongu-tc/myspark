package leongu.myspark._business.assetanalysis.util

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.immutable.{ListMap, TreeMap}


trait AACons {
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
  val ASSET_ANALYSIS_JOB = "asset_analysis_job"
  val BULKLOAD_DIR = "bulkload_dir"

  // cons
  val HALF_DAY = 12 * 3600 * 1000
  val HBASE_CF = "cf"
  val HBASE_CF_BYTES = Bytes.toBytes(HBASE_CF)

  val pDecimal = "(decimal.*)".r
}
