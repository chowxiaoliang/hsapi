package sql.batch

import hbase.{CoreOperationService, CoreOperationServiceImpl}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object RiskEventMonthReportS {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RiskEventReportS")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sql("use anti")
    val result = sparkSession.sql("SELECT PARTNER_ID, EVENT_TYPE,count(*) as sum FROM RISK_EVENT_HIVE WHERE OCCUR_TIME_STR >= '2020-06-01 00:00:00' and OCCUR_TIME_STR <= '2020-06-30 59:59:59' GROUP BY PARTNER_ID , EVENT_TYPE")
    val map = new mutable.HashMap[String, mutable.HashMap[String, Int]]()
    result.foreach(x => {
      val partnerId = x.getString(0)
      val eventType = x.getString(1)
      val sum = x.getInt(2)
      calMap(map, partnerId, eventType, sum)
    })
    // 写数据
    insertMonthData(map, sparkSession)
  }

  /**
   * 最终的结果 xinwangbank_yyyy_MM login_10:loan_20
   * @param data
   */
  def insertMonthData(data : mutable.HashMap[String, mutable.HashMap[String, Int]], sparkSession: SparkSession): Unit ={
    if(data != null && data.nonEmpty){
      val coreOperationService: CoreOperationService = new CoreOperationServiceImpl()
      data.keySet.foreach(x => {
        val partnerId = x
        val eventTypeMap = data.get(partnerId)
        if(eventTypeMap != null && eventTypeMap.nonEmpty){
          val eventStr = ""
          eventTypeMap.get.keySet.foreach(y => {
            val eventType = y
            val sum = eventTypeMap.get.get(eventType)
            eventStr.concat(eventType).concat("_").concat(String.valueOf(sum)).concat(":")
          })
          // 写到数据库
          val rowKey = partnerId.concat("_2020-06")
          coreOperationService.addRowData("RISK_EVENT_MONTH_REPORT", rowKey, "INFO", "RESULT", eventStr)
        }
      })
    }
  }

  /**
   * 计算对应的关系 map(partnerId, (eventType, sum))
   * @param sourceMap
   * @param partnerId
   * @param eventType
   * @param sum
   */
  def calMap(sourceMap : mutable.HashMap[String, mutable.HashMap[String, Int]], partnerId : String, eventType : String, sum : Int) : Unit = {
    val eventTypeMap = sourceMap.get(partnerId)
    if(eventTypeMap == null){
      val eventTypeMap = mutable.HashMap[String, Int]()
      eventTypeMap.put(eventType, sum)
      sourceMap.put(partnerId, eventTypeMap)
    }else{
      val innerSum = eventTypeMap.get(eventType)
      eventTypeMap.get.put(eventType, innerSum + sum)
    }
  }
}
