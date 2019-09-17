package leongu.myspark.sql.hbase

import java.text.DecimalFormat
import java.util.{ArrayList, List, Random}

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

object Spark2HBaseJavaApi {
  val ZOOKEEPER_ADDRESS = "localhost"
  val ZOOKEEPER_PORT = "2182"
  val df2: DecimalFormat = new DecimalFormat("00")

  def main(args: Array[String]) = {
    val tableName: String = "test01"
    val conn = getConn
    val admin = conn.getAdmin
    val putList = getPutList()
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      createTable(admin, tableName, Array("cf"))
    }
    val start: Long = System.currentTimeMillis
    insertBatchData(conn, tableName, admin, putList)
    val end: Long = System.currentTimeMillis
    System.out.println("用时：" + (end - start))
  }

  def getConn(): Connection = {
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_ADDRESS)
    conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT)
    ConnectionFactory.createConnection(conf)
  }

  def insertBatchData(conn: Connection, tableName: String, admin: Admin, puts: List[Put]) = try {
    val tableNameObj = TableName.valueOf(tableName)
    if (admin.tableExists(tableNameObj)) {
      val table = conn.getTable(tableNameObj)
      table.put(puts)
      table.close()
      admin.close()
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  }

  def createTable(admin: Admin, tableName: String, colFamiles: Array[String]) = try {
    val tableNameObj = TableName.valueOf(tableName)
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(tableNameObj)
      for (colFamily <- colFamiles) {
        desc.addFamily(new HColumnDescriptor(colFamily))
      }
      admin.createTable(desc)
      admin.close()
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  }

  def getPutList(): List[Put] = {
    val random: Random = new Random
    val putlist = new ArrayList[Put]();
    for (i <- 0 until 100000) {
      val rowkey: String = df2.format(random.nextInt(99)) + i
      val put: Put = new Put(rowkey.getBytes)
      put.add("cf".getBytes, "field".getBytes, "a".getBytes)
      putlist.add(put)
    }
    putlist
  }
}
