package leongu.myspark._business.rt_asset.adjusting

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object AdjustingBolt extends Adjusting with Serializable {
  def initialAdjust(spark: SparkSession, conf: mutable.Map[String, Object]): Unit = {
    logInfo("INITIAL Adjusting beginning ... ...")
    adjusting(spark, conf, conf.getOrElse(ADJUSTING_DAY, "20190902").toString)
    logInfo("INITIAL Adjusting finished ... ...")
  }

  def dailyAdjust(): Unit = {
    // TODO
  }

  def main(args: Array[String]): Unit = {
    stk_asset_schema.map(kv => println(kv._1 + " " + kv._2))
  }
}
