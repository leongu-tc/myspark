package leongu.myspark.session.api

import leongu.myspark.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object DataSetApi extends Logging {
  Logger.getLogger("org").setLevel(Level.WARN)

  case class Person(name: String, gender: String, age: Int, addr: String, col1: String, id: Int)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("dataset_api")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.sql.warehouse.dir", "/user/spark/spark-warehouse") // 不论warehouse在哪里hive都可以访问其数据
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("SELECT * FROM db1.t3")
    df.show()

    catalyst(spark, df)
    operator(spark, df)
    extendFun(spark, df)
    encode(spark, df)
    other(spark, df)

    println("Over!")
  }

  /**
    * 为了能把外部数据转换为 spark sql 内部使用
    * implicit 是 SparkSession 的内部对象，参见 SQLImplicits Encoders
    * 将java scala的基础类型转换为 ExpressionEncoder 可以和 Row 互换
    *
    * @param spark
    * @param df
    * @see ExpressionEncoder SQLImplicits Encoders
    */
  def encode(spark: SparkSession, df: DataFrame) = {
    println("====================Encode==================== ")
    import spark.implicits._
    val ds = Seq(1, 2, 3).toDS() // implicitly provided (spark.implicits.newIntEncoder)
  }

  def catalyst(spark: SparkSession, df: DataFrame) = {
    println("====================catalyst==================== ")
    df.explain()
    df.explain(true) // parsed logical plan -> analyzed logical plan -> Optimized Logical plan -> Physical Plan
    df.dtypes.foreach(x => println("-" + x._1 + " " + x._2))
    println(df.isLocal) // 如果collect take方法可以本地运行 不依赖任何 executors ?
    println(df.isEmpty) // 触发了action 进行汇聚运算了
    println(df.isStreaming)
    //    df.toDF()
    // as need implicits
    import spark.implicits._
    df.as[Person].show() // @see DataSet.apply

    df.printSchema()
    df.schema.printTreeString()

    df.inputFiles.foreach(println(_))

    // warehouse
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src1 (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH '" + Constants.prefix + "kv1.txt' INTO TABLE src1")

    //    spark.conf.set("spark.sql.warehouse.dir", "/user/spark/spark-warehouse") // AnalysisException  static config: spark.sql.warehouse.dir

    df.withColumnRenamed("name", "NAME1").show(1) // RENAME
    df.drop("name", "addr").show(1) // RENAME
    df.withColumn("ID1", $"id").show // 使用 id 字段的值新建一个 Column ID1, 只能是本 DataSet的字段；
    df.withColumn("name", $"id").show // 如果同名字段存在则替换

    df.createOrReplaceTempView("v1")
    spark.sql("select * from v1").show
    df.createOrReplaceGlobalTempView("gv1")

  }

  def other(spark: SparkSession, df: DataFrame) = {
    println("====================other==================== ")
    df.show(false) // FALSE 表示不会把长多超过 20 字符的结果进行截取
    df.show(20, 0, true) // row num, 0 表示不截取 > 0表示前N个字符, 最后vertical是否垂直展示
    df.dropDuplicates("name").show()
    df.distinct().show()

    /** 1、checkpoint 可以用来 truncate 截断 logical plan,
      * 和后面的流计算的checkpoint 类似的都是为了 truncate，但是实现略有不同 */
    spark.sparkContext.setCheckpointDir("hdfs://localhost:9000/tmp/checkpoints/datasetapi")
    df.checkpoint() // save to checkpointDir，未设置则报错 tmp/checkpoints/datasetapi/{jobid}/rdd-{rddid}
    df.checkpoint(false) // only mkdir, but no data util actions
    df.localCheckpoint // 本地存储，对于迭代运算速度很快，但是数据完整性无法保障

    df.withWatermark("col1", "1 minute") // streaming 专用的； 1 防止状态算子无限扩大，2 window

    /** 2、填充null字段,e.g. col1 字段值为null，我们用 na 就可以找到他们，然后选择 drop fill replace 等,最后持久化就OK */
    df.na.fill("na").show

    /** 3、stat 数据分析：bloomfilter countminSketch sampleBy freqItems crosstab corr cov approxQuantile */
    df.stat.approxQuantile("age", Array(0.5, 0.9), 0.05).map(println(_)) // 近似数量
    println(df.stat.cov("age", "id").toString) // 协方差
    println(df.stat.corr("age", "id")) // 皮尔森相关系数
    df.stat.freqItems(Array("name", "age"), 0.4).show // 字段出现频率超过 0.4 的所有值的集合
    // countminSketch 就不展示了
    val bf = df.stat.bloomFilter("name", 10, 100)
    Seq("andy", "wangwu", "wanger").foreach(r => println(bf.mightContainString(r))) // bf 可以快速过滤集合，对非 false的再进行判断，减少很多计算

    /** 4、analyst, 注意这里和 CBO 没有关系，执行完后 DESCRIBE EXTENDED 看不到 Stats信息，CBO 需要的是 Analyst 源表 而不是 DataSet */
    df.describe("id").show()
    df.describe().show // 表示所有的 number 和 string 字段
    /** 更加详细的指标 summary， 默认 count, mean, stddev, min,
      * approximate quartiles (percentiles at 25%, 50%, and 75%), and max.
      */
    df.summary().show // 默认是所有  number和String 字段的
    df.select("id", "name").summary().show // 选定字段的

    /** 5 persist */
    df.persist(StorageLevel.MEMORY_AND_DISK_SER_2).show()
    df.toJSON.show
  }

  def operator(spark: SparkSession, df: DataFrame) = {
    println("====================operator==================== ")

    // Column need implictits
    import spark.implicits._
    // functions need sql.functions._
    import org.apache.spark.sql.functions._

    val df1 = spark.sql("SELECT * FROM db1.t1")


    /** 1、join */

    df.join(df1, Seq("id"), "inner").show // 这里即使 t1 t2 的 join 字段类型不同但是可以cast为commontype，因此可以查询，参见built-in function的==
    df.join(df1, Seq("id"), "leftouter").show // 参见 JoinType
    df.join(df1, Seq("id"), "rightouter").show
    df.crossJoin(df1).show // 笛卡尔乘积 ，不需要配置 spark.sql.crossJoin.enabled=truedf.join(df1.hint("broadcast"), Seq("id"), "rightouter").show

    // Experimental

    spark.conf.set("spark.sql.crossJoin.enabled", "true") // 否则 Detected implicit cartesian product for LEFT OUTER join between logical plans
    val jdf = df.joinWith(df1, $"db1.t1.id" > 1, "leftouter") // 实际上也是一个 笛卡尔乘积，加上了condition过滤
    jdf.printSchema()
    jdf.show(false) // return tuple2

    /** 2、sort */
    df.sortWithinPartitions($"name" desc, $"age" desc).show(false) // = HIVEQL SORY BY
    df.sort($"name".desc, $"age" desc).show(false) // = orderBy

    /** 3、column */
    df.apply("db1.t3.id") // Column apply col colRegex 和 $"db1.id" 没有任何区别, 只是不需要 implicits
    println(df.col("db1.t3.id").toString())

    // as 必须在一个job里面才能识别，下面再使用就无法识别了
    df.join(df1.as("alias1"), Seq("id"), "rightouter")
      .selectExpr("db1.t3.name", "alias1.name").show
    df.selectExpr("id", "name as newName", "abs(age)").show()
    import spark.implicits._
    df.select($"id" + 100).show
    df.select("id", "name").show()
    df.filter($"id" > 2).show // = where

    /** 4、group */
    // @see RelationalGroupedDataset

    df.groupBy($"gender", $"addr")
      .agg(Map(
        "age" -> "avg", // 被覆盖掉了，这样不行 Map
        "age" -> "max"
      )).show

    // tuple 好很多，起码不会被覆盖
    df.groupBy($"gender", $"addr")
      .agg(
        "age" -> "avg",
        "age" -> "max"
      ).show

    // must import org.apache.spark.sql.functions._
    df.groupBy($"gender", $"addr")
      .agg($"gender", $"addr" // $"gender", $"addr", will repeated
        , count("*"), max("age") + 1000)
      .show()

    df.groupBy($"gender", $"addr").count().show()
    df.groupBy($"gender", $"addr").mean("id", "age").show()
    df.agg(count("*"), max("age") + 1000).show // 全表统计 short for groupBy().agg

    // pivot 透视函数，行列转换
    // 特别适合处理 年 月 按年分行，按月分列（pivot 字段值变列名）然后汇聚每一个月的值到每一个月的col上
    df.groupBy($"gender").pivot("addr").avg("age").show

    df.rollup($"gender", $"addr")
      .agg($"gender", $"addr" // $"gender", $"addr", will repeated
        , count("*"), max("age"))
      .show()
    df.cube($"gender", $"addr")
      .agg($"gender", $"addr" // $"gender", $"addr", will repeated
        , count("*"), max("age"))
      .show()

    df.union(df1).show() // df1 按照 df 的字段顺序进行union，不考虑 df1 的字段名
    df.unionByName(df1).show() // df1 按照 自己的字段名跟 df 进行union，

    df.intersect(df1).show() // 会去重
    df.intersectAll(df1).show() // 这里不会去重，t3 t1 交集是2行并且这2行完全相同，会展示2行
    // 如果没有 此函数我们可以 e.g. 加上 ROW_NUMBER 来区分

    df.except(df1).show() // 结果会去重 = EXCEPT DISTINCT
    df.exceptAll(df1).show() // 结果不会去重

    var sdf = df.sample(true, 0.1, 100) // true表示抽取放回 ??? TODO
    sdf.show()

    df.randomSplit(Array(0.5, 0.5)).map(_.show())

    df.select($"*", explode(split($"addr", " ")).as("exp")).show
  }

  /**
    * 感觉 reduce 和 groupByKey 这样的数据比较适合 DataSet 而非 DataFrame，因为 DataFrame 本身是结构化的数据
    * 有很多 built-in 方法可以使用；但是 typed 比 Row更加灵活
    *
    * @param spark
    * @param df
    * @return
    */
  def extendFun(spark: SparkSession, df: DataFrame) = {
    println("====================extendFun==================== ")
    import spark.implicits._
    val ds = df.as[Person]
    /** 1 reduce groupByKey filter map flatMap foreach transform */
    // reduce ???


    // see KeyValueGroupedDataset
    val keyedDf = ds.groupByKey(person => person.id % 3)
    keyedDf.count().show()

    var filterFn = (col: Column, y: Int) => col.<(y) // OR USE FilterFunction
    df.filter(filterFn($"id", 5)).show

    var mapFn = (r: Row) => r.getAs[Int]("age").+(10) // Or MapFunction
    df.map(mapFn).show

    var flatmapFn = (r: Row) => {
      r.getAs[String]("addr").split(" ")
        .map(e => (r.getAs[String]("name"), e)).iterator
    } // Or MapFunction
    df.flatMap(flatmapFn).toDF("name", "addr1").show

    var foreachFn = (r:Row) => println(r)
    df.foreach(foreachFn)
//    df.foreachPartition(iter => iter.foreach(foreachFn))
    var foreachPartitionFn = (iter:Iterator[Row]) => iter.foreach(foreachFn)
    df.foreachPartition(foreachPartitionFn)

    import spark.implicits._ // createDataSet need implicits
    df.transform(rows => spark.createDataset(Seq(("A", 1), ("B", 2))).toDF("name", "age")).show()

    /** 2 driver */
    // 本地化（zepplin livy是用它返回数据么）：
    // toLocalIterator 将 partition 一个个拿到Driver，不知道这个有啥用，不过起码比collect消耗少很多
    // 注意 join的时候 RDD 最好先cache
    val iter = df.toLocalIterator()
    while (iter.hasNext) {
      println(iter.next)
    }

    df.limit(1).show()
    df.head(2).foreach(println(_)) // head = first = take

    /** 3 partition */
    df.repartition(3, $"name").show
    df.repartitionByRange(3, $"name" desc).show // 分区建range
    df.coalesce(1).show()

  }
}
