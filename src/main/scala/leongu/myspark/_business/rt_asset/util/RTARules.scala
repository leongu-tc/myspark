package leongu.myspark._business.rt_asset.util

import org.apache.hadoop.hbase.util.Bytes

object RTARules extends RTACons {

  /**
    * stock balance match add
    */
  def stk_compute(bsflag: String, stkbal: Long, matchqty: Long): Long = {
    var ret = stkbal
    println("1 -------:" + ret.toString)
    bsflag match {
      case "03" | "0B" | "0a" | "0b" | "0c" | "0d" | "0e" | "0q" | "1j" | "3m" => ret = ret + matchqty
      case "04" | "0S" | "0f" | "0g" | "0h" | "0i" | "0j" | "0r" | "3n" => ret = ret - matchqty
    }
    println("-------:" + ret.toString)
    ret
  }

  val BS_FLAG_STK = List("03", "04", "0B", "0S",
    "0a", "0b", "0c", "0d", "0e", "0f", "0g", "0h", "0i", "0j",
    "0q", "0r", "1j", "3m", "3n"
  )

  def main(args: Array[String]) = {
    val a = Bytes.toBytes(9L);
    println(a.length)
    println(Bytes.toLong(a))
  }
}
