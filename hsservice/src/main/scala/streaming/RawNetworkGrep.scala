package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}

object RawNetworkGrep {

  def main(args: Array[String]): Unit = {
    /**
     * Receives text from multiple rawNetworkStreams and counts how many '\n' delimited
     * lines have the word 'the' in them. This is useful for benchmarking purposes. This
     * will only work with spark.streaming.util.RawTextSender running on all worker nodes
     * and with Spark using Kryo serialization (set Java property "spark.serializer" to
     * "org.apache.spark.serializer.KryoSerializer").
     * Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis>
     *   <numStream> is the number rawNetworkStreams, which should be same as number
     *               of work nodes in the cluster
     *   <host> is "localhost".
     *   <port> is the port on which RawTextSender is running in the worker nodes.
     *   <batchMillise> is the Spark Streaming batch duration in milliseconds.
     */
    if (args.length != 4) {
      System.err.println("Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis>")
      System.exit(1)
    }

    val Array(numStreams, host, port, batchMillis) = args
    val sparkConf = new SparkConf().setAppName("RawNetworkGrep").setMaster("local[4]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Duration(batchMillis.toLong))
    ssc.sparkContext.setLogLevel("WARN")

    val rawStreams = (1 to numStreams.toInt).map(_ =>
      ssc.rawSocketStream[String](host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)).toArray
    val union = ssc.union(rawStreams)
    union.filter(_.contains("the")).count().foreachRDD(r =>
      println(s"Grep count: ${r.collect().mkString}"))
    ssc.start()
    ssc.awaitTermination()
  }

}
