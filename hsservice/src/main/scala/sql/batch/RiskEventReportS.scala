package sql.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RiskEventReportS {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RiskEventReportS")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sql("use RISK_EVENT_HIVE")
    sparkSession.sql("select * from RISK_EVENT_HIVE where ")

  }
}
