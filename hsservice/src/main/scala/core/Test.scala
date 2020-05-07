package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/4/29 0029.
  */
object Test {

  def main(args: Array[String]): Unit = {
    print("hello world!")

    val sparkConf = new SparkConf();

    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.parallelize(List("1"), 3)
  }

}
