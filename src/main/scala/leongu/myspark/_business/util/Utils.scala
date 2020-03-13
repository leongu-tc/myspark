package leongu.myspark._business.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.internal.Logging
import org.yaml.snakeyaml.Yaml

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object Utils extends Logging with Cons {

  def readConf(file: String): mutable.Map[String, Object] = {
    var conf: mutable.Map[String, Object] = mutable.Map()
    if (file != null) {
      val stream = Source.fromFile(file).reader()
      val yaml = new Yaml()
      var javaConf = yaml.load(stream).asInstanceOf[java.util.HashMap[String, Object]]
      conf = JavaConverters.mapAsScalaMapConverter(javaConf).asScala
    }

    logInfo("------------ Config -----------")
    conf.map(kv => logInfo(kv.toString()))
    logInfo("------------ End --------------")
    conf
  }


  def yesterday(): Calendar = {
    val day = Calendar.getInstance()
    day.add(Calendar.DATE, -1)
    day
  }

  def yesterdayStr(df:SimpleDateFormat): String = {
    df.format(yesterday.getTime)
  }

  /**
   * azkaban's SDP_DATA_TIME (yyyyMMdd) has top priority
   * jobDate -1: full history
   * not set: use yesterday
   * @param conf
   * @param jobDateConf
   * @param df jobDate format
   * @return also jobDate format
   */
  def jobDateFn(conf: mutable.Map[String, Object], jobDateConf: String, df:SimpleDateFormat): String = {
    if (conf.contains(jobDateConf)) {
      conf.getOrElse(jobDateConf, "-1").toString // use the config date
    } else {
      // 1 use SDP_DATE_TIME, 2 use yesterday
      var sdpDataTimeStr = System.getenv("SDP_DATA_TIME")
      logInfo(s"SDP_DATA_TIME $sdpDataTimeStr")
      if (sdpDataTimeStr != null && sdpDataTimeStr.length > 0) {
        df.format(DF1.parse(sdpDataTimeStr)).toString
      } else {
        yesterdayStr(df)
      }
    }
  }
}
