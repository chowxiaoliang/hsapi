package sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JdbcDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("JdbcDemo").master("local[1]").getOrCreate()
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password","QA@skyroam")
    properties.setProperty("driver","com.mysql.jdbc.Driver")

    val mysqlDataFrame = sparkSession.read.jdbc("jdbc:mysql://192.168.9.42:3306/engine", "engine_rule", properties)

    mysqlDataFrame.createOrReplaceTempView("t_rule")
    val selectDataFrame = sparkSession.sql("select rule_content from t_rule where id = 12")
    selectDataFrame.describe().show()

    sparkSession.stop()

  }

}
