package leongu.myspark.session.streaming

import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Row => _, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._

object KafkaExample extends Logging {

  case class Person(id: Long, name: String, age: Long, ts: Timestamp)

  val servers = "localhost:9092"
  val pub = "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO"
  val pri = "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB"
  val topic = "topic1"

  //  val servers = "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667"
  //  val pub = "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO"
  //  val pri = "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB"
  //  val topic = "topic1"

  //  val servers = "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667"
  //  val pub = "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO"
  //  val pri = "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB"
  //  val topic = "topic1"

  val hbase_quorum = "localhost"
  val hbase_zk_port = "2182"
  val hbase_zk_parent = "/hbase"
  val hbase_pub = ""
  val hbase_pri = ""
  val hbase_user = "hbase"
  val hbase_tbl = "t1"

  //  val hbase_quorum = "sdp-10-88-100-140,sdp-10-88-100-141,sdp-10-88-100-142"
  //  val hbase_zk_port = "2181"
  //  val hbase_zk_parent = "/hbase-unsecure"
  //  val hbase_pub = "ItC2TbwGpXPHK9lCS5cGEWI7tzH8AoAnLKtJ"
  //  val hbase_pri = "pc0mO6NCjixoMZf9FSjJeVHADP6sng9T"
  //  val hbase_user = "hbase"
  //  val hbase_tbl = "t1"

  def main(args: Array[String]) {
    // For windows
    // System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop-common-2.2.0-bin-master")

    Logger.getLogger("org").setLevel(Level.WARN)

    //    System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop-common-2.2.0-bin-master")


    val spark = SparkSession
      .builder()
      // 指定spark集群
      //      .master("spark://localhost:7077")
      .master("local")
      .appName("Kafka example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      //      .option("kafka.kafka.security.authentication.sdp.publickey", pub)
      //      .option("kafka.kafka.security.authentication.sdp.privatekey", pri)
      //      .option("kafka.security.protocol", "SASL_SDP")
      //      .option("kafka.sasl.mechanism", "SDP")
      .option("failOnDataLoss", "false") //参数 数据丢失，false表示工作不被禁止，会从checkpoint中获取找到断电，从断点开始从新读数据
      .option("max.poll.records", 10000)
      .option("subscribe", topic)
      .load()

    df.printSchema()

    //    kafkatopic(spark, df)
    //    kafkatokafkatopic(spark, df)
    //    kafkatohbase(spark, df)
    kafkatohbase2(spark, df)


    println("done!")
  }

  def kafkatopic(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val lineRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    //处理成Row
    val ds = lineRDD.map(_.split(","))
      .map(attributes => {
        try {
          Person(attributes(0).trim.toLong, attributes(1), attributes(2).trim.toInt, new Timestamp(attributes(3).trim.toLong))
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person(0, "Nil", 0, new Timestamp(0l))
          }
        }
      })

    val rowdf = ds.toDF()
    rowdf.printSchema()

    // Start running the query that prints the running counts to the console
    val query = rowdf.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "checkpoints") // HDFS 保存 checkpoint 的方式
      .start()
    query.awaitTermination()
  }

  def kafkatokafkatopic(spark: SparkSession, df: DataFrame): Unit = {
    val query = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "topic2")
      //      .option("kafka.bootstrap.servers", "10.88.100.140:6667,10.88.100.141:6667,10.88.100.142:6667")
      //      .option("kafka.kafka.security.authentication.sdp.publickey", "M2D3lAtKDtCM63kD7i8xYbSieX5EZ73xIevO")
      //      .option("kafka.kafka.security.authentication.sdp.privatekey", "rZSDd3EiCyvGjEz6UUoFvafY8VyOYhMB")
      //      .option("kafka.security.protocol", "SASL_SDP")
      //      .option("kafka.sasl.mechanism", "SDP")
      .option("checkpointLocation", "checkpoints")
      .start()

    query.awaitTermination()
  }

  def kafkatohbase(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val lineRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    //处理成Row
    val ds = lineRDD.map(_.split(","))
      .map(attributes => {
        try {
          Person(attributes(0).trim.toLong, attributes(1), attributes(2).trim.toInt, new Timestamp(attributes(3).trim.toLong))
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person(0, "Nil", 0, new Timestamp(0L))
          }
        }
      })

    val rowdf = ds.toDF()
    rowdf.printSchema()

    val query = rowdf.writeStream
      .queryName("toHbase")
      .outputMode("append")
      .foreach(
        new ForeachWriter[Row] {
          var hbaseConf: Configuration = _
          var connection: Connection = _
          var table: Table = _

          def open(partitionId: Long, version: Long): Boolean = {
            println("create hbase client ------------------- start")
            hbaseConf = HBaseConfiguration.create()
            hbaseConf.set("hbase.zookeeper.quorum", hbase_quorum)
            hbaseConf.set("hbase.zookeeper.property.clientPort", hbase_zk_port)
            //          hbaseConf.set("zookeeper.znode.parent", hbase_zk_parent)
            //          hbaseConf.set("hbase.security.authentication.sdp.publickey", hbase_pub)
            //          hbaseConf.set("hbase.security.authentication.sdp.privatekey", hbase_pri)
            //          hbaseConf.set("hbase.security.authentication.sdp.username", hbase_user)

            connection = ConnectionFactory.createConnection(hbaseConf)
            table = connection.getTable(TableName.valueOf(hbase_tbl))
//            println("create hbase client ------------------- start")
            true
          }

          def process(record: Row): Unit = {
            println(record.toString)
            println(record.get(0).toString)
            println(record.get(1).toString)
            println("------------")
            val theput = new Put(Bytes.toBytes("rk_" + record.get(0).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(record.get(0).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(record.get(1).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(record.get(2).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ts"), Bytes.toBytes(record.get(3).toString))
            table.put(theput)
          }

          def close(errorOrNull: Throwable): Unit = {
            println("close hbase connection -----------------")
            connection.close()
          }
        }
      )
      .option("checkpointLocation", "checkpoints")
      .start()

    query.awaitTermination()
  }


  def kafkatohbase2(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val lineRDD: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    //处理成Row
    val ds = lineRDD.map(_.split(","))
      .map(attributes => {
        try {
          Person(attributes(0).trim.toLong, attributes(1), attributes(2).trim.toInt, new Timestamp(attributes(3).trim.toLong))
        }
        catch {
          case e1: Exception => {
            println(attributes)
            Person(0, "Nil", 0, new Timestamp(0L))
          }
        }
      })

    val rowdf = ds.toDF().withWatermark("ts", "10 seconds").dropDuplicates()
    rowdf.printSchema()

    val query = rowdf.writeStream
      .queryName("toHbase")
      .outputMode("append")
      .format("console")
      /*.foreachBatch { (df: DataFrame, bid: Long) =>
        df.foreachPartition(records => {
//          println(s"create hbase client ------------------- start, bid = $bid")
          var hbaseConf = HBaseConfiguration.create()
          hbaseConf.set("hbase.zookeeper.quorum", hbase_quorum)
          hbaseConf.set("hbase.zookeeper.property.clientPort", hbase_zk_port)
          //          hbaseConf.set("zookeeper.znode.parent", hbase_zk_parent)
          //          hbaseConf.set("hbase.security.authentication.sdp.publickey", hbase_pub)
          //          hbaseConf.set("hbase.security.authentication.sdp.privatekey", hbase_pri)
          //          hbaseConf.set("hbase.security.authentication.sdp.username", hbase_user)

          var connection = ConnectionFactory.createConnection(hbaseConf)
          var table = connection.getTable(TableName.valueOf(hbase_tbl))
//          println("create hbase client ------------------- start")
          records.foreach(record => {
            println(record.toString)
//            println(record.get(0).toString)
//            println(record.get(1).toString)
//            println("------------")
            val theput = new Put(Bytes.toBytes("rk_" + record.get(0).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(record.get(0).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(record.get(1).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(record.get(2).toString))
            theput.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ts"), Bytes.toBytes(record.get(3).toString))
            table.put(theput)
          })
//          println("close hbase connection -----------------")
          connection.close()
        })
      }*/
      .option("checkpointLocation", "checkpoints")
      .start()

    query.awaitTermination()
  }

}
