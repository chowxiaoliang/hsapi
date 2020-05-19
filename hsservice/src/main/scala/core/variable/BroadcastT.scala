package core.variable

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastT {

  def main(args: Array[String]): Unit = {
    /**
     * 广播变量
     */
    val sparkConf = new SparkConf()
    sparkConf.setAppName("BroadcastT").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkConf)

    val broadV = sparkContext.broadcast(Array(1,2,3))
    broadV.value.foreach(println)
  }

}
