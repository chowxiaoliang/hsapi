package core

import org.apache.spark.{SparkConf, SparkContext}

object TwoPairRddT {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[1]")
    sparkConf.setAppName("TwoPairRddT")
    val sparkContext = new SparkContext(sparkConf)
    /**
     * 时间戳，省份，城市，用户，广告。中间使用空格分隔
     *
     * 1516609143867 6 7 64 16
     * 1516609143869 9 4 75 18
     * 1516609143869 1 7 87 12
     */
    /**
     * 需求：统计每一个省份点击TOP3的广告ID 省份+广告
     */
    //2.读取数据生成 RDD： TS， Province， City， User， AD
    val line = sparkContext.textFile("D:\\bigdata\\test.txt")
    //3.按照最小粒度聚合： ((Province,AD),1)
    val provinceAdToOne = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    //4.计算每个省中每个广告被点击的总数： ((Province,AD),sum)
    val provinceAdToSum = provinceAdToOne.reduceByKey(_ + _)
    //5.将省份作为 key，广告加点击数为 value： (Province,(AD,sum))
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))
    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup = provinceToAdSum.groupByKey()
    //7.对同一个省份所有广告的集合进行排序并取前 3 条，排序规则为广告点击总数
    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }
    //8.将数据拉取到 Driver 端并打印
    provinceAdTop3.collect().foreach(println)
    //9.关闭与 spark 的连接
    sparkContext.stop()
  }

}
