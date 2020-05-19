package core.variable

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorT {

  def main(args: Array[String]): Unit = {

    /**
     * 累加器
     */
    val sparkConf = new SparkConf()
    sparkConf.setAppName("AccumulatorT").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkConf)

    val accum = sparkContext.longAccumulator("AccumulatorT")

    sparkContext.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))

    println(accum.value)
  }

}
