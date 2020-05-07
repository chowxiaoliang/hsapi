package core

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @author zhouliang
  * scala编写的wordcound
  */
object WcAppS {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
    sparkConf.setAppName("wordcoud")

    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile("E:\\myproject\\bigdata\\inputOne\\wordcount-spark.txt")

    val words = lines.flatMap(x => x.split(","))

    words.mapPartitions(x => {
      val list = new ListBuffer[String]
      list.iterator
    }
    )

    val counts = words.map(w => (w, 1)).reduceByKey((x, y) => x + y)

    counts.saveAsTextFile("E:\\myproject\\bigdata\\inputOne\\wordcount-spark-result-scala.txt")


  }

}
