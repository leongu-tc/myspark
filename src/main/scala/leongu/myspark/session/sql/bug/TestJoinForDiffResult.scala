package leongu.myspark.session.sql.bug

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * 结果是因为 Hive 对于char(n) 不对补空串，但是SparkSQL 会，stkasset 的stkcode是 char(8) 的，而 stkprice 的 stkcode 是 String的；
 */
object TestJoinForDiffResult extends Logging {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      // 指定spark集群
      //      .master("spark://localhost:7077")
      .master("local")
      .appName("TestJoinForDiffResult")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    val sS = sql("select  S.STKCODE from martrd.STKASSET S ")
    sS.show()

    //    s.limit(2).foreach(r => println(r.prettyJson))
    //    val s = sql("select P.LASTPRICE, P.STKCODE, P.MARKET from centrd.STKPRICE P")
    //    s.show()
    //    val s1 = s.filter("STKCODE = '000709'")
    //    s1.show()

    sql(
      """
        SELECT /*+ BROADCAST(S) */ S.ORGID
         ,S.FUNDID
         ,S.MONEYTYPE
         ,S.CUSTID
         FROM martrd.STKASSET S
           INNER JOIN centrd.STKPRICE P -- 这里,表示的是 INNER JOIN
         ON S.STKCODE = P.STKCODE -- RBO 自动识别为 ON 条件，因此是一个SortMerge 而非 Cartesian
         AND S.MARKET =P.MARKET
         WHERE P.bizDATE='20200708'
         AND S.bizDATE='20200708'
      """).show()

    val s1 = sql("select S.ORGID,S.FUNDID,S.MONEYTYPE,S.CUSTID, S.STKCODE, S.MARKET from martrd.STKASSET S")
    s1.show()
    val s2 = sql("select S.ORGID,S.FUNDID,S.MONEYTYPE,S.CUSTID, S.STKCODE, S.MARKET from martrd.STKASSET S WHERE STKCODE < '000709'")
    s2.show()
    val s = sql("select S.ORGID,S.FUNDID,S.MONEYTYPE,S.CUSTID, S.STKCODE, S.MARKET from martrd.STKASSET S WHERE STKCODE = '000709'")
    s.show()
    val p = sql("select P.LASTPRICE, P.STKCODE, P.MARKET from centrd.STKPRICE P WHERE STKCODE='000709'")
    p.show()
    val ret = s.join(p, Seq("MARKET", "STKCODE"))
    ret.show()

    spark.stop()
    println("done!")
  }

}
