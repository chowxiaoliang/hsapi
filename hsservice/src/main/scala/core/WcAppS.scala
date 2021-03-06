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
    sparkConf.setAppName("wordcount")

    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile("D:\\bigdata\\wordcount.txt")

    val words = lines.flatMap(x => x.split(","))

//    val counts = words.map(w => (w, 1)).reduceByKey((x, y) => x + y)
//    counts.saveAsTextFile("D:\\bigdata\\wordcount-result.txt")

    val counts = words.countByValue();
    counts.foreach(x => println(x))

  }

}
