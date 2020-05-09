package core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/4/29 0029.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Test")
    sparkConf.setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf)
//    val rdd = sparkContext.parallelize(List(1,2,3,4,5))
//    val rddT = sparkContext.parallelize(List(4,5,6,7,8))

    val rdd = sparkContext.parallelize(List((1,2),(2,3),(3,4)))
    val rddT = sparkContext.parallelize(List((2,5),(3,5),(3,6),(4,6)))
    val cogroup = rdd.cogroup(rddT)
    cogroup.foreach(println)
  }
}
