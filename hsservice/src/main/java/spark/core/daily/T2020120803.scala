package spark.core.daily

import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import spark.core.selfdefine.MyPartitioner

object T2020120803 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("T2020120803").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val path = "D:\\bigdata\\wordcount.txt";
    val rdd : RDD[String] = sparkContext.textFile(path, 3)
    val flatMapRdd = rdd.flatMap(s => s.split(" "))
    val mapRdd = flatMapRdd.map(s => Tuple2(s, 1))
//    mapRdd.partitionBy(new RangePartitioner(3, mapRdd, true))
    val partitionRdd = mapRdd.partitionBy(new MyPartitioner(4))
    val result = partitionRdd.collect();
    result.foreach(x => print(x))
  }

}
