package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("QueueStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue)

    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_+_)
    reducedStream.print()
    ssc.start()

    for(i <- 1 to 30){
      rddQueue.synchronized{
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(1000)
    }
    ssc.stop()
  }

}
