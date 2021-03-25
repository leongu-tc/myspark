package leongu.myspark.rdd

import org.apache.spark._

object WordCountBig {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount")
      //.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("hdfs://localhost:9000/user/spark/my/csv/table.csv", 10)
    val words = lines.flatMap(line => line.split("\t"))
    val pairs = words.map(word => (word, 1))
    val wordcounts = pairs.reduceByKey(_ + _)

    // for each
//    wordcounts.foreach(wordcount => println(wordcount._1 + " appeared " + wordcount._2 + " times"))
    // for each partition
    wordcounts.foreachPartition(_ => println("A"))
    wordcounts.take(1)
    Thread.sleep(10000)
    sc.stop()
  }
}
