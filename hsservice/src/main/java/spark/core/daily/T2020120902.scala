package spark.core.daily

import org.apache.spark.{SparkConf, SparkContext}

/**
 * map join
 * Map-side Join使用场景是一个大表和一个小表的连接操作，其中，“小表”是指文件足够小，可以加载到内存中。该算法可以将join算子执行在Map端，无需经历shuffle和reduce等阶段，因此效率非常高。
 * 在Hadoop MapReduce中， map-side join是借助DistributedCache实现的。DistributedCache可以帮我们将小文件分发到各个节点的Task工作目录下，这样，我们只需在程序中将文件加载到内存中
 * （比如保存到Map数据结构中），然后借助Mapper的迭代机制，遍历另一个大表中的每一条记录，并查找是否在小表中，如果在则输出，否则跳过。
 * 在Apache Spark中，同样存在类似于DistributedCache的功能，称为“广播变量”（Broadcast variable）。其实现原理与DistributedCache非常类似，
 * 但提供了更多的数据/文件广播算法，包括高效的P2P算法，该算法在节点数目非常多的场景下，效率远远好于DistributedCache这种基于HDFS共享存储的方式，
 * 具体比较可参考“Performance and Scalability of Broadcast in Spark”。使用MapReduce DistributedCache时，用户需要显示地使用File API编写程序从本地读取小表数据，而Spark则不用，
 * 它借助Scala语言强大的函数闭包特性，可以隐藏数据/文件广播过程，让用户编写程序更加简单。
 * 假设两个文件，一小一大，且格式类似为：
 */
object T2020120902 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("T2020120902")
    val sparkContext = new SparkContext(sparkConf)
    val table1 = sparkContext.textFile(args(1))
    val table2 = sparkContext.textFile(args(2))

    // table1 is smaller, so broadcast it as a map<String, String>
    val pairs = table1.map { x =>
      val pos = x.indexOf(',')

      (x.substring(0, pos), x.substring(pos + 1))

    }.collectAsMap

    val broadCastMap = sparkContext.broadcast(pairs) //save table1 as map, and broadcast it

    // table2 join table1 in map side
    val result = table2.map { x =>
      val pos = x.indexOf(',')

      (x.substring(0, pos), x.substring(pos + 1))

    }.mapPartitions({ iter =>
      val m = broadCastMap.value
      for{
        (key, value) <- iter
        if(m.contains(key))
      } yield (key, (value, m.getOrElse(key, "")))
    })

    result.saveAsTextFile(args(3)) //save result to local file or HDFS
  }
}
