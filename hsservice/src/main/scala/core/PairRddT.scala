package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PairRddT {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("pairRddT")
    sparkConf.setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf)
//    val rdd = sparkContext.parallelize(List((1,2),(3,4),(3,6)))

//    val reduceByKey = rdd.reduceByKey(_+_)
//    val reduceByKey = rdd.reduceByKey((x,y) => x + y)
//    reduceByKey.foreach(println)

//    val groupByKey = rdd.groupByKey()
//    groupByKey.foreach(println)

//    val aggregateByKey = rdd.aggregateByKey(3)((x,y) => math.max(x, y), (z, m) => z + m)
//    aggregateByKey.foreach(println)

//    val foldByKey = rdd.foldByKey(3)(_+_)
//    foldByKey.foreach(println)

//    val countByKey = rdd.countByKey()
//    countByKey.foreach(println)

//    val countByValue = rdd.countByValue()
//    countByValue.foreach(println)

    // 这个排序作用在元组上
//    val rddT = sparkContext.parallelize(List(Tuple2(1,2),Tuple2(2,2),Tuple2(3,4)))
//    val sortByKey = rddT.sortByKey(ascending=true)
//    sortByKey.foreach(println)

//    val keys = rdd.keys;
//    keys.foreach(println)
//
//    val values = rdd.values;
//    values.foreach(println)

//    val mapValues = rdd.mapValues(x => x*x)
//    mapValues.foreach(println)

//    val flatMapValues = rdd.flatMapValues(x => x to 5)
//    flatMapValues.foreach(println)

//    val collectAsMap = rdd.collectAsMap();
//    collectAsMap.foreach(x => println(x))

//    val lookup = rdd.lookup(1)
//    lookup.foreach(println)

//    val rddT = sparkContext.parallelize(List(Tuple2("panda",0),Tuple2("pink",3),Tuple2("pirate",3),Tuple2("panda",1),Tuple2("pink",4)))
//    val result = rddT.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
//    result.foreach(println)

//    val data = sparkContext.parallelize(List(("1","3"),("1","2"),("1","5"),("2","3")))
//    val natPairRdd = data.combineByKey(List(_), (c: List[String], v: String) => v::c, (c1: List[String], c2: List[String]) => c1 ::: c2)
//    val result = natPairRdd.collect
//    result.foreach(print)

    val rdd = sparkContext.parallelize(
      Array(("George", 88.0), ("George", 95.0), ("George", 88.0),("KangKang", 93.0), ("KangKang", 95.0), ("KangKang", 98.0),("limu", 98.0)))
    /**
     * createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
     * mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
     * mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
     */
    val res = rdd.combineByKey(
      x => {println(s"$x******");x},
      (x: Double, y: Double) => {println(s"$x%%%%%%$y");x+y},
      (x: Double, y: Double) => {println(s"$x@@@@@@$y");x+y}
    )
    res.foreach(println)
  }
}
