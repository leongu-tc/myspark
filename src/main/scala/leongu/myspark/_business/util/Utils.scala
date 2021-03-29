package leongu.myspark._business.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Objects}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
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


  def readConfFromResources(file: String): mutable.Map[String, Object] = {
    var conf: mutable.Map[String, Object] = mutable.Map()
    if (file != null) {
      val stream = Source.fromURL(getClass.getResource(file)).bufferedReader()
      if (Objects.isNull(stream)) {
        throw new Exception("can not read " + file)
      }
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

  /**
   * 类型和 Constants 中的 types 的 values 对应
   *
   * @See Constants
   * @param ftype
   * @return
   */
  def mkSparkType(ftype: String): DataType = {
    ftype.toLowerCase match {
      case "string" => StringType
      case "integer" => IntegerType
      case "long" => LongType
      case "double" => DoubleType
      case "boolean" => BooleanType
      case "byte" => ByteType
      case "byte[]" => BinaryType
      case "short" => ShortType
      case "float" => FloatType
      case "java.math.bigdecimal" => DecimalType(24, 6)
      case "bigdecimal" => DecimalType(24, 6)
      case "java.sql.date" => DateType
      case "date" => DateType // supporting "0001-01-01" through "9999-12-31".
      case "java.sql.timestamp" => TimestampType
      case "timestamp" => TimestampType
      case _ => {
        throw new Exception(s"type: $ftype unsupported yet!")
      }
    }
  }
}
