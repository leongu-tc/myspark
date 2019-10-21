package leongu.myspark._business.rt_asset.util

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.immutable.{ListMap, TreeMap}


trait RTACons {
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
  val HBASE_TBL_STK = "hbase_tbl_stk"
  val HBASE_TBL_FUND = "hbase_tbl_fund"
  val HBASE_TBL_OF = "hbase_tbl_of"

  val ADJUSTING_DAY = "adjusting_day"
  val BULKLOAD_DIR = "bulkload_dir"

  // cons
  val HALF_DAY = 12 * 3600 * 1000
  val HBASE_CF = "cf"
  val HBASE_CF_BYTES = Bytes.toBytes(HBASE_CF)

  /** schema */
  // 1 for long, 2 for string, 3 for bigdecimal

  // @Deprecated
  val match_schema = ListMap(
    "serverid" -> 1, "matchsno" -> 1, "operdate" -> 1, "custid" -> 1, "fundid" -> 1,
    "moneytype" -> 2, "fundkind" -> 2, "fundlevel" -> 2, "fundgroup" -> 2, "orgid" -> 2,
    "brhid" -> 2, "secuid" -> 2, "rptsecuid" -> 2, "bsflag" -> 2, "rptbs" -> 2,
    "matchtype" -> 2, "ordersno" -> 2, "orderid" -> 2, "market" -> 2, "stkcode" -> 2,
    "stkname" -> 2, "stktype" -> 2, "trdid" -> 2, "orderprice" -> 3, "bondintr" -> 3,
    "orderqty" -> 1, "seat" -> 2, "matchtime" -> 1, "matchprice" -> 3, "matchqty" -> 1,
    "matchamt" -> 3, "matchcode" -> 2, "clearamt" -> 3, "operid" -> 1, "operlevel" -> 2,
    "operorg" -> 2, "operway" -> 2, "bankcode" -> 2, "bankbranch" -> 2, "banknetplace" -> 2,
    "sourcetype" -> 2, "recnum" -> 1, "bankorderid" -> 2, "bankid" -> 2, "exteffectamt" -> 3,
    "bankrtnflag" -> 2, "remark" -> 2, "creditid" -> 2, "creditflag" -> 2, "trddate" -> 1
  )

  val stk_asset_schema = TreeMap(
    "serverid" -> 1, "custid" -> 1, "orgid" -> 2, "fundid" -> 1, "moneytype" -> 2,
    "market" -> 2, "secuid" -> 2, "seat" -> 2, "stkcode" -> 2, "stklastbal" -> 1,
    "stkbal" -> 1, "stkavl" -> 1, "stkbuy" -> 1, "stksale" -> 1, "stkbuysale" -> 1,
    "stkuncomebuy" -> 1, "stkuncomesale" -> 1, "stkfrz" -> 1, "stkunfrz" -> 1, "stknightfrz" -> 1,
    "stktrdfrz" -> 1, "stktrdunfrz" -> 1, "stkdiff" -> 1, "stksalediff" -> 1, "stkremain" -> 1,
    "stkcorpremain" -> 1, "creditstkbal" -> 1, "creditstkbuysale" -> 1, "stkflag" -> 2, "lastbuycost" -> 3,
    "lastprofitcost" -> 3, "buycost" -> 3, "profitcost" -> 3, "mktval" -> 3, "stkavl_in" -> 1,
    "stkavl_out" -> 1, "stkbuysale2" -> 1, "stkdecimal" -> 3, "bizdate" -> 1)


  val fund_asset_schema = TreeMap(
    "serverid" -> 1, "orgid" -> 2, "fundseq" -> 1, "fundid" -> 1, "moneytype" -> 2,
    "custid" -> 1, "fundlastbal" -> 3, "fundbal" -> 3, "fundavl" -> 3, "overdraw" -> 3,
    "fundbuy" -> 3, "fundsale" -> 3, "funduncomebuy" -> 3, "funduncomesale" -> 3, "fundfrz" -> 3,
    "fundunfrz" -> 3, "fundtrdfrz" -> 3, "fundtrdunfrz" -> 3, "fundnightfrz" -> 3, "fundloan" -> 3,
    "creditbal" -> 3, "creditbuysale" -> 3, "creditavl" -> 3, "creditbuy" -> 3, "creditsale" -> 3,
    "creditamtused" -> 3, "fundflag" -> 2, "otherasset" -> 3, "marketvalue" -> 3, "fundstandby" -> 3,
    "fundbuysale" -> 3, "fundbalprefrz" -> 3, "busi_date" -> 2)

}
