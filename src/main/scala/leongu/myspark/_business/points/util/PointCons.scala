package leongu.myspark._business.points.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.util.Bytes

trait PointCons {
  /** config */
  val KAFKA_SERVERS = "kafka_servers"
  val KAFKA_PUBKEY = "kafka_pub"
  val KAFKA_PRIKEY = "kafka_pri"
  val KAFKA_TOPIC_POINT = "kafka_topic_point"
  val KAFKA_PROTOCOL = "kafka_protocol"
  val KAFKA_MECHANISM = "kafka_mechanism"


  val HBASE_QUORUM = "hbase_quorum"
  val HBASE_ZK_PORT = "hbase_zk_port"
  val HBASE_ZK_PARENT = "hbase_zk_parent"
  val HBASE_PUBKEY = "hbase_pub"
  val HBASE_PRIKEY = "hbase_pri"
  val HBASE_USER = "hbase_user"
  val HBASE_TBL_POINT = "hbase_tbl_point"

  /** cons */
  val HBASE_CF = "cf"
  val HBASE_CF_BYTES = Bytes.toBytes(HBASE_CF)

  def yesterday(): Calendar = {
    val day = Calendar.getInstance()
    day.add(Calendar.DATE, -1)
    day
  }

  /** sql */
  lazy val logDate = new SimpleDateFormat("yyyyMMdd").format(yesterday.getTime)
  val individual_cust = "C" // cust_id,cust_telno
  // individual custom
  val CUSTBASEINFO_SQL = s"SELECT custid as cust_id,mobileno as cust_telno FROM centrd.custbaseinfo WHERE busi_date='$logDate' AND singleflag = 0"
  val busi_sqls = List(
    s""" SELECT CU.cust_id, CU.cust_telno, '010109' as busi_no, '$logDate' as busi_date, C2.orgid
       |(SELECT C.cust_id,C.cust_telno LEFT JOIN kbssacct.user_basic_info as U ON C.cust_id == U.user_code WHERE U.open_source = 1) as CU
       | LEFT JOIN centrd.customer as C2 ON CU.cust_id == C2.custid WHERE C2.busi_date='$logDate'"
     """.stripMargin
  )

}
