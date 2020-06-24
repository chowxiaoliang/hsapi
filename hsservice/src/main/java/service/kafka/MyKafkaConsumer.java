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

        CoreOperationService coreOperationService = new CoreOperationServiceImpl();

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = CONSUMER.poll(100);

            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                String jsonData = record.value();
                JSONObject jsonObject = JSONObject.parseObject(jsonData);
                String rowKey = jsonObject.getString("riskFlowFlowNo");
                try {
                    coreOperationService.addBatchRowData("RISK_EVENT", rowKey, jsonObject);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
