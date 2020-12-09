package spark.core.daily

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduce join
 * 当两个文件/目录中的数据非常大，难以将某一个存放到内存中时，Reduce-side Join是一种解决思路。该算法需要通过Map和Reduce两个阶段完成，
 * 在Map阶段，将key相同的记录划分给同一个Reduce Task（需标记每条记录的来源，便于在Reduce阶段合并），在Reduce阶段，对key相同的进行合并。
 * Spark提供了Join算子，可以直接通过该算子实现reduce-side join，但要求RDD中的记录必须是pair，即RDD[KEY, VALUE]，
 * 同样前一个例利用Reduce-side join实现如下：
 */
object T2020120903 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("T2020120903").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val table1 = sparkContext.textFile(args(1))
    val table2 = sparkContext.textFile(args(2))

    val pairs = table1.map{x =>
      val pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }

    val result = table2.map{x =>
      val pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }.join(pairs)

    result.saveAsTextFile(args(3))
  }

}
