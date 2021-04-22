package leongu.myspark._business.points.util

import java.text.SimpleDateFormat
import java.util.Calendar

import leongu.myspark._business.points.Points.conf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.util.StringUtils

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

  val LOG_DATE = "log_date"
  val COMMENDER_NAMES = "commender_names"

  /** cons */
  val HBASE_CF = "cf"
  val HBASE_CF_BYTES = Bytes.toBytes(HBASE_CF)

  def yesterday(): Calendar = {
    val day = Calendar.getInstance()
    day.add(Calendar.DATE, -1)
    day
  }

  /** sql */
  lazy val logDate = logDateFn()

  def logDateFn(): String = {
    if (conf.contains(LOG_DATE)) {
      conf.getOrElse(LOG_DATE, "-1").toString
    } else {
      var ret_date = conf.getOrElse(LOG_DATE, new SimpleDateFormat("yyyyMMdd").format(yesterday.getTime)).toString
      var dataTime = System.getenv("SDP_DATA_TIME")
      println(s"------SDP_DATA_TIME $dataTime")
      if (dataTime != null && dataTime.length > 0) {
        val sdp_date = dataTime.substring(0, 4).concat(dataTime.substring(4, 6)).concat(dataTime.substring(6, 8))
        if (sdp_date < ret_date) {
          ret_date = sdp_date
        }
      }
      ret_date
    }
  }

  lazy val commenders = conf.getOrElse(COMMENDER_NAMES, "").toString.split(",").map(e => "'" + e + "'").mkString(",")
  val individual_cust = "C" // cust_id,cust_telno
  // individual custom
  lazy val CUSTBASEINFO_SQL = s"SELECT string(custid) as cust_id,mobileno as cust_telno FROM centrd.custbaseinfo WHERE singleflag = 0"
  lazy val busi_sqls = List(
    s""" SELECT CU.cust_id, CU.cust_telno, '010109' as busi_no, '$logDate' as busi_date, C2.orgid
       | FROM (SELECT string(C.custid) as cust_id,C.mobileno as cust_telno
       |        FROM centrd.custbaseinfo C INNER JOIN zh20.user_basic_info as U
       |        ON C.custid == U.user_code
       |        WHERE U.open_source = 1 AND C.opendate = '$logDate' AND C.singleflag = 0) as CU
       | LEFT JOIN centrd.customer as C2 ON CU.cust_id == C2.custid
     """.stripMargin,
    s"""
       | SELECT CCU.cust_id, CCU.cust_telno, '010110' as busi_no, '$logDate' as busi_date, CCU.orgid, C3.commender_name
       | FROM (SELECT CU.cust_id, CU.cust_telno, C2.orgid
       |        FROM (SELECT string(C.custid) as cust_id,C.mobileno as cust_telno
       |                FROM centrd.custbaseinfo C INNER JOIN zh20.user_basic_info as U
       |                ON C.custid == U.user_code
       |                WHERE U.open_source = 1 AND C.opendate = '$logDate' AND C.singleflag = 0) as CU
       |        LEFT JOIN centrd.customer as C2 ON CU.cust_id == C2.custid) AS CCU
       | INNER JOIN yzt.oa_s_cczq C3 ON CCU.cust_id = C3.cust_code
       | WHERE trim(C3.commender_name) IN ($commenders)
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010201' as busi_no, '$logDate' as busi_date
       | FROM centrd.cgemsecuinfo U INNER JOIN C
       | ON C.cust_id = U.custid
       | WHERE dealresult = '00' AND U.signdate='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010301' as busi_no, '$logDate' as busi_date
       | FROM C INNER JOIN zh20.cust_invest_pro U
       | ON C.cust_id = U.cust_code
       | WHERE U.prof_investor_type='1' AND U.prof_sign_date='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010401' as busi_no, '$logDate' as busi_date
       | FROM centrd.secuid U INNER JOIN C
       | ON C.cust_id = U.custid
       | WHERE U.securight in ('0w') AND U.status = 0 and U.opendate='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010501' as busi_no, '$logDate' as busi_date
       | FROM C INNER JOIN ygt.opp_busi_data U
       | ON C.cust_id = U.cust_code
       | WHERE U.busi_code='D0008' AND U.proc_status='13'
       | AND replace(to_date(U.oper_time),'-','')= '$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010601' as busi_no, '$logDate' as busi_date
       | FROM C INNER JOIN ygt.opp_busi_data U
       | ON C.cust_id = U.cust_code
       | WHERE U.busi_code='D0009' AND U.proc_status='13'
       | AND replace(to_date(U.oper_time),'-','')= '$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010701' as busi_no, '$logDate' as busi_date
       | FROM C INNER JOIN zh20.cust_agreement U
       | ON C.cust_id = U.cust_code
       | WHERE U.cust_agmt_type = '11' and U.eft_date='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010801' as busi_no, '$logDate' as busi_date
       | FROM C INNER JOIN zh20.private_fund_invest U
       | ON C.cust_id = U.cust_code
       | WHERE U.fund_invest_type = '1' AND U.apply_date='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'010901' as busi_no, '$logDate' as busi_date
       |  FROM zh20.cust_agreement U INNER JOIN C
       |  ON C.cust_id = U.cust_code
       |  WHERE U.cust_agmt_type = '26' AND U.eft_date='$logDate'
     """.stripMargin,
    //    s"""SELECT C.cust_id,C.cust_telno,'011001' as busi_no, '$logDate' as busi_date, U.market
    //       |  FROM centrd.secuid U INNER JOIN C
    //       |  ON C.cust_id = U.custid
    //       |  WHERE U.securight IN ('0s') AND U.status = 0 AND U.opendate='$logDate'
    //     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'011001' as busi_no, '$logDate' as busi_date,
       |  element_at(map('5', '沪港通', 'S', '深港通', '5,S', 'both', 'S,5', 'both'), U2.markets) as market
       |  FROM (SELECT U.custid, concat_ws(',' ,collect_set(U.market)) AS markets
       |        FROM centrd.secuid U
       |        where U.securight in ('0s') AND U.status = 0 AND U.opendate='$logDate' GROUP BY custid)
       |  AS U2 INNER JOIN C
       |  ON C.cust_id = U2.custid
    """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'011101' as busi_no, '$logDate' as busi_date
       |  FROM martrd.cdtapplication U INNER JOIN C
       |  ON C.cust_id = U.custid
       |  WHERE effectivedate='$logDate' AND U.status='5'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'011201' as busi_no, '$logDate' as busi_date
       |  FROM centrd.secuid U INNER JOIN C
       |  ON C.cust_id = U.custid
       |  WHERE U.securight in ('05','06') AND U.status = 0 AND U.opendate='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'011301' as busi_no, '$logDate' as busi_date
       |  FROM jz61.opt_trdacct U INNER JOIN C ON C.cust_id = U.cust_code
       |  WHERE U.effect_date='$logDate'
     """.stripMargin,
    s"""SELECT C.cust_id,C.cust_telno,'011401' AS busi_no,'$logDate' AS busi_date, ordersno, fundcode
       |  FROM C INNER JOIN
       |  (SELECT custid AS cust_id, ordersno, ofcode AS fundcode
       |     FROM centrd.ofmatch WHERE trdid='240059' AND busi_date='$logDate'
       |  UNION
       |  SELECT V.cust_code AS cust_id, V.app_sno AS ordersno, W.inst_id AS fundcode
       |     FROM otc41.otc_auto_invest_agr V LEFT JOIN otc41.otc_inst_base_info W
       |     ON V.inst_sno = W.inst_sno
       |     WHERE V.agr_stat='1' AND V.app_date='$logDate'
       |  ) U ON C.cust_id = U.cust_id
     """.stripMargin,
    s"""SELECT DISTINCT C.cust_id,C.cust_telno, '011403' AS busi_no,'$logDate' AS busi_date
       |	FROM C INNER JOIN
       |		(SELECT DISTINCT a.custid FROM
       |			(SELECT * FROM centrd.eccodesign
       |				WHERE busi_date='$logDate' AND orderdate=$logDate AND fundcode in ('000905','000861','002325') AND multisettstatus='0' AND isnewsign='1')a
       |		LEFT JOIN
       |			(SELECT * FROM centrd.eccodesign
       |        WHERE busi_date='$logDate' AND updatedate=$logDate AND fundcode in ('000905','000861','002325') AND multisettstatus<>'0')b
       |		ON a.custid=b.custid
       |		WHERE b.custid is null
       |		) U
       |	ON C.cust_id = U.custid
    """.stripMargin,
    s"""SELECT DISTINCT C.cust_id,C.cust_telno, '011404' AS busi_no,'$logDate' AS busi_date
       |	FROM C INNER JOIN
       |		(SELECT DISTINCT a.custid FROM
       |			(SELECT * FROM centrd.eccodesign
       |				WHERE busi_date='$logDate' AND orderdate=$logDate AND fundcode in ('000905','000861','002325') AND multisettstatus='0')a
       |		LEFT JOIN
       |			(SELECT * FROM centrd.eccodesign
       |				WHERE busi_date='$logDate' AND updatedate=$logDate AND fundcode in ('000905','000861','002325') AND multisettstatus<>'0')b
       |		ON a.custid=b.custid
       |		WHERE b.custid is not null AND a.fundcode<>b.fundcode
       |		) U
       |	ON C.cust_id = U.custid
    """.stripMargin,
    s"""SELECT DISTINCT C.cust_id,C.cust_telno, '011405' AS busi_no,'$logDate' AS busi_date
       |	FROM C INNER JOIN
       |		(SELECT custid,orderdate FROM centrd.eccustsign WHERE orderdate=$logDate
       |		) U
       |	ON C.cust_id = U.custid
    """.stripMargin,
    s"""SELECT DISTINCT C.cust_id,C.cust_telno, '011406' AS busi_no,'$logDate' AS busi_date
       |	FROM C INNER JOIN
       |		(SELECT custid,busi_date FROM centrd.oforder
       |      WHERE busi_date='$logDate' AND operdate=$logDate AND trdid='240029'
       |    UNION
       |    SELECT cust_code AS custid,busi_date FROM otc41.otc_trd_orders
       |      WHERE busi_date='$logDate' AND app_date=$logDate AND trd_id='129'
       |		) U
       |	ON C.cust_id = U.custid
    """.stripMargin,
    s"""SELECT CU.cust_id,CU.cust_telno,'011407' AS busi_no, '$logDate' AS busi_date
       |	FROM
       |	(SELECT DISTINCT C.cust_id,C.cust_telno, U.apply_date
       |		FROM C INNER JOIN zh20.private_fund_invest U
       |	ON C.cust_id = U.cust_code
       |	WHERE U.fund_invest_type = '4'AND U.apply_date='$logDate') AS CU
    """.stripMargin,
    s"""SELECT DISTINCT C.cust_id,C.cust_telno, '012101' AS busi_no,'$logDate' AS busi_date
       |        FROM C LEFT JOIN
       |                (SELECT custid,busi_date FROM centrd.logbanktran
       |						WHERE busi_date='$logDate' AND banktranid='1' AND status='2'
       |                union
       |                SELECT custid,busi_date FROM martrd.logbanktran
       |						WHERE busi_date='$logDate' AND banktranid='1' AND status='2' ) U
       |				ON C.cust_id = U.custid
       |				left join
       |				(SELECT custid,busi_date FROM centrd.logbanktran
       |						WHERE busi_date<'$logDate' AND busi_date>'20200101' AND banktranid='1' AND status='2'
       |                union
       |                SELECT custid,busi_date FROM martrd.logbanktran
       |						WHERE busi_date<'$logDate' AND busi_date>'20200101' AND banktranid='1' AND status='2' ) V
       |				ON C.cust_id = V.custid
       |        WHERE U.custid IS NOT NULL AND V.custid IS NULL
    """.stripMargin
  )

}
