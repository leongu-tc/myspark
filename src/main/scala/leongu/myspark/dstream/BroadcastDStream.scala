package leongu.myspark.dstream

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BroadcastDStream {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    val trans = lines.transform(stream => {
      BroadcastWrapper.getInstance().updateAndGet(stream.context)
      stream
    })
    // Split each line into words
    val words = trans.flatMap(_.split(" ")) // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    //    wordCounts.print()
    wordCounts.foreachRDD(_.foreach(wc => println(wc + " --- " + BroadcastWrapper.getInstance().getVarValue())))
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
