package service.kafka;

import bean.RiskEvent;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import hbase.CoreOperationService;
import hbase.CoreOperationServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MyKafkaConsumer {

    private static KafkaConsumer<String, String> CONSUMER ;

    private static final String TOPIC = "risk";

    static {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "192.168.11.200:9092");
        // 制定consumer group
        props.put("group.id", "risk");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        CONSUMER = new KafkaConsumer<>(props);
        // 消费者订阅的topic, 可同时订阅多个
        CONSUMER.subscribe(Collections.singletonList(TOPIC));
    }

    public static void consumerMessage(){

//        CoreOperationService coreOperationService = new CoreOperationServiceImpl();

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = CONSUMER.poll(100);

            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                String jsonData = record.value();
                JSONObject jsonObject = JSONObject.parseObject(jsonData);

                Map<String, String> resultMap = new HashMap<>(10);
                // 分columnFamily

                // baseInfo
                RiskEvent riskEventBaseInfo = new RiskEvent();
                riskEventBaseInfo.setPartnerId(jsonObject.getString("partnerId"));
                riskEventBaseInfo.setRiskFlowNo(jsonObject.getString("riskFlowNo"));
                riskEventBaseInfo.setCertNo(jsonObject.getString("certNo"));
                riskEventBaseInfo.setName(jsonObject.getString("name"));
                riskEventBaseInfo.setMobile(jsonObject.getString("mobile"));
                riskEventBaseInfo.setBankCard(jsonObject.getString("bankCard"));
                riskEventBaseInfo.setCountry(jsonObject.getString("country"));
                riskEventBaseInfo.setProvince(jsonObject.getString("province"));
                riskEventBaseInfo.setCity(jsonObject.getString("city"));
                riskEventBaseInfo.setOccurTimeDate(jsonObject.getDate("occurTimeDate"));
                riskEventBaseInfo.setOccurTimeLong(jsonObject.getLongValue("occurTimeLong"));
                riskEventBaseInfo.setOccurTimeStr(jsonObject.getString("occurTimeStr"));
                resultMap.put("baseInfo", JSONObject.toJSONString(riskEventBaseInfo));

                // deviceInfo
                RiskEvent riskEventDeviceInfo = new RiskEvent();
                riskEventDeviceInfo.setMac(jsonObject.getString("mac"));
                riskEventDeviceInfo.setImei(jsonObject.getString("imei"));
                riskEventDeviceInfo.setPhoneOperator(jsonObject.getString("phoneOperator"));
                riskEventDeviceInfo.setOs(jsonObject.getString("os"));
                resultMap.put("deviceInfo", JSONObject.toJSONString(riskEventDeviceInfo));

                // deliverInfo
                RiskEvent riskEventDeliverInfo = new RiskEvent();
                riskEventDeliverInfo.setDeliverAddressCountry(jsonObject.getString("deliverAddressCountry"));
                riskEventDeliverInfo.setDeliverAddressProvince(jsonObject.getString("deliverAddressProvince"));
                riskEventDeliverInfo.setDeliverAddressCity(jsonObject.getString("deliverAddressCity"));
                riskEventDeliverInfo.setDeliverZipCode(jsonObject.getString("deliverZipCode"));
                riskEventDeliverInfo.setDeliverMobileNo(jsonObject.getString("deliverMobileNo"));
                riskEventDeliverInfo.setDeliverName(jsonObject.getString("deliverName"));
                riskEventDeliverInfo.setBankName(jsonObject.getString("bankName"));
                riskEventDeliverInfo.setBankCardNo(jsonObject.getString("bankCardNo"));
                riskEventDeliverInfo.setOrigOrderId(jsonObject.getString("origOrderId"));
                resultMap.put("deliveryInfo", JSONObject.toJSONString(riskEventDeliverInfo));

                // sellerInfo
                RiskEvent riskEventSellerInfo = new RiskEvent();
                riskEventSellerInfo.setPayeeName(jsonObject.getString("payeeName"));
                riskEventSellerInfo.setPayeeEmail(jsonObject.getString("payeeEmail"));
                riskEventSellerInfo.setPayeeMobile(jsonObject.getString("payeeMobile"));
                riskEventSellerInfo.setPayeeIdNumber(jsonObject.getString("payeeIdNumber"));
                riskEventSellerInfo.setPayeeCardNumber(jsonObject.getString("payeeCardNumber"));
                riskEventSellerInfo.setPayId(jsonObject.getString("payId"));
                riskEventSellerInfo.setPayMethod(jsonObject.getString("payMethod"));
                resultMap.put("sellerInfo", JSONObject.toJSONString(riskEventSellerInfo));

                System.out.println(JSONObject.toJSONString(resultMap));
//                String rowKey = jsonObject.getString("riskFlowFlowNo");
//                try {
//                    coreOperationService.addBatchRowData("RISK_EVENT", rowKey, resultMap);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
            }
        }
    }
}
