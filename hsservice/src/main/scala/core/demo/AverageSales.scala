package core.demo

import org.apache.spark.{SparkConf, SparkContext}

object AverageSales {

  def main(args: Array[String]): Unit = {
    /**
     * 给定一组键值对("spark",2),("hadoop",6),("hadoop",4),("spark",6)，键值对的key
     * 表示图书名称，value表示某天图书销量，请计算每个键对应的平均值，也就是计算每种图书的每天平均销量。
     */
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[1]")
    sparkConf.setAppName("AverageSales")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(List(("spark",2),("hadoop",6),("hadoop",4),("spark",6)))
//    val mapRdd = rdd.map(x => (x._1,(x._2,1)))
//    val groupRdd = mapRdd.groupByKey().mapValues(v => {
//      var values = 0;var days = 0;v.foreach(x => {values = values + x._1;days = days + x._2});values/days})
//    groupRdd.foreach(println)

    rdd.mapValues(x => (x,1)).reduceByKey((x,y)=> (x._1+y._1,x._2+y._2)).mapValues(x => x._1/x._2).foreach(println)

  }

}
