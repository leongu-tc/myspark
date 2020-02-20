package leongu.myspark._business.points.external

import leongu.myspark._business.points.util.PointCons
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.parsing.json.JSONObject

object ExTools extends PointCons {

  def getHBaseVal(t: Table, rk: String, cols: Seq[String]): Seq[Cell] = {
    val get = new Get(Bytes.toBytes(rk))
    val ret = t.get(get)
    for (col <- cols) yield ret.getColumnLatestCell(HBASE_CF_BYTES, Bytes.toBytes(col))
  }

  def getHBaseLongVal(c: Cell): Long = {
    Bytes.toLong(c.getValueArray, c.getValueOffset, c.getValueLength)
  }

  def getHBaseStringVal(c: Cell): String = {
    Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength)
  }

  def getHBaseDecimalVal(c: Cell): BigDecimal = {
    Bytes.toBigDecimal(c.getValueArray, c.getValueOffset, c.getValueLength)
  }

  def jsonValue(map: Map[String, String]): JSONObject = {
    JSONObject.apply(map)
  }

  def hbasePut(rowkey: String, map: Map[String, String]): Put = {
    val theput = new Put(Bytes.toBytes(rowkey))
    map.map(e => theput.addColumn(HBASE_CF_BYTES, Bytes.toBytes(e._1), Bytes.toBytes(e._2)))
    theput
  }

  def main(args: Array[String]): Unit = {
//    val map: Map[String, String] = Map("A" -> "B", "C" -> null)
val map: Map[String, String] = Map("A" -> "B", "C" -> null)
    println(jsonValue(map.filter(_._2 != null)))
  }
}
