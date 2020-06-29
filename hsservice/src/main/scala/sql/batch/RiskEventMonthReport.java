package sql.batch;

import hbase.CoreOperationService;
import hbase.CoreOperationServiceImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhouliang
 * @desc 月报表
 */
public class RiskEventMonthReport {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[3]").setAppName("RiskEventMonthReport");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("use anti");
        Dataset<Row> dataset = sparkSession.sql("SELECT PARTNER_ID, EVENT_TYPE,count(*) as sum FROM RISK_EVENT_HIVE WHERE OCCUR_TIME_STR >= '2020-06-01 00:00:00' and OCCUR_TIME_STR <= '2020-06-30 59:59:59' GROUP BY PARTNER_ID , EVENT_TYPE");
        Map<String, Map<String, Integer>> map = new HashMap<>(100);
        for (Row row : dataset.collectAsList()){
            String partnerId = row.getString(0);
            String eventType = row.getString(1);
            int sum = row.getInt(2);
            calMap(map, partnerId, eventType, sum);
        }
        insertMonthData(map, sparkSession);
    }

    /**
     * 最终的结果 xinwangbank_yyyy_MM login_10:loan_20
     * @param map
     * @param sparkSession
     * @throws IOException
     */
    private static void insertMonthData(Map<String, Map<String, Integer>> map, SparkSession sparkSession) throws IOException {
        if(map != null && map.size() > 0){
            CoreOperationService coreOperationService = new CoreOperationServiceImpl();
            for(String partnerId : map.keySet()){
                Map<String, Integer> eventTypeMap = map.get(partnerId);
                if(eventTypeMap != null && eventTypeMap.size() > 0){
                    String eventStr = null;
                    for(String eventType : eventTypeMap.keySet()){
                        int sum = eventTypeMap.get(eventType);
                        eventStr = eventType.concat("_").concat(sum+"").concat(":");
                    }
                    // 写到数据库
                    String rowKey = partnerId.concat("_2020-06");
                    coreOperationService.addRowData("RISK_EVENT_MONTH_REPORT", rowKey, "INFO", "RESULT", eventStr);
                }
            }
        }
    }

    /**
     * 计算对应的关系 map(partnerId, (eventType, sum))
     * @param sourceMap
     * @param partnerId
     * @param eventType
     * @param sum
     */
    private static void calMap(Map<String, Map<String, Integer>> sourceMap, String partnerId, String eventType, int sum){
        Map<String, Integer> eventTypeMap = sourceMap.get(partnerId);
        if(eventTypeMap == null){
            eventTypeMap = new HashMap<>(100);
            eventTypeMap.put(eventType, sum);
            sourceMap.put(partnerId, eventTypeMap);
        }else{
            int innerSum = eventTypeMap.get(eventType);
            eventTypeMap.put(eventType, innerSum + sum);
        }
    }
}
