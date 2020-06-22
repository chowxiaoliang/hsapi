package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object FirstDemo {

  def main(args: Array[String]): Unit = {
    /**
     * 创建DataFrame
     * id name age
     * 1 张三 23
     * 2 李四 25
     */

    val sparkSession = SparkSession.builder().appName("demo").master("local[1]").getOrCreate()
    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("info")

    val rdd = sparkContext.parallelize(List((1,"张三",23),(2,"李四",25)))
    val peopleRdd:RDD[Row] = rdd.map(x => Row(x._1,x._2,x._3))
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
    StructField("name", StringType, false),
    StructField("age", IntegerType, false)))

    val peopleDFf = sparkSession.createDataFrame(peopleRdd, schema)
    peopleDFf.show()

    peopleDFf.createOrReplaceTempView("t_people")
    sparkSession.sql("select * from t_people").show()
    val result = sparkSession.sql("select * from t_people")
//    result.foreach(x => {
//      println(x.getString(1))
//    })
    result.take(2).foreach(x => println(x))
    sparkSession.stop()

  }

  case class Person(id:Int,name:String,age:Int) extends Serializable
}
