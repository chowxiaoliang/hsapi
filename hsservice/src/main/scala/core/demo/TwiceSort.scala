package core.demo

import org.apache.spark.{SparkConf, SparkContext}

object TwiceSort {

  def main(args: Array[String]): Unit = {
    /**
     * 二次排序
     * 题目：要求先按账户排序，在按金额排序
     *     hadoop@apache          200
     *     hive@apache            550
     *     yarn@apache            580
     *     hive@apache            159
     *     hadoop@apache          300
     *     hive@apache            258
     *     hadoop@apache          150
     *     yarn@apache            560
     *     yarn@apache            260
     *
     * 结果：(hadoop@apache,[150,200,300]),(hive@apache,[159,258,550]),....
     */
    val sparkConf = new SparkConf();
    sparkConf.setAppName("TwiceSort")
    sparkConf.setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf);
    val rdd = sparkContext.parallelize(List(("hadoop@apache",200),("hive@apache",550),("yarn@apache",580),
      ("hive@apache",159),("hadoop@apache",300),("hive@apache",258),("hadoop@apache",150),("yarn@apache",560),("yarn@apache",260)))

    val resultRdd = rdd.groupByKey().mapValues(x => x.toList.sortBy(x => x)).sortByKey(false)
    resultRdd.foreach(println)
  }

}
