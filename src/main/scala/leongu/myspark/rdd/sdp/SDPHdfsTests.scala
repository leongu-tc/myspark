package leongu.myspark.rdd.sdp

import org.apache.spark.{SparkConf, SparkContext}

object SDPHdfsTests {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SDPHdfsTests")
      //.setMaster("local")
    val sc = new SparkContext(sparkConf)
    val peopleDF = sc.textFile("hdfs://hdfsCluster/user/spark/my/people.json")
    peopleDF.saveAsTextFile("hdfs://hdfsCluster/user/spark/my/people2.json")
  }
}
