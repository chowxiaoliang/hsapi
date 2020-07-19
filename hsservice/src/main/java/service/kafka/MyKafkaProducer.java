package service.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class MyKafkaProducer {

    private static Producer<String, String> PRODUCER ;

    private static final String TOPIC = "risk";

    static {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop001:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        PRODUCER = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        // 不带回调函数
//        for (int i = 0; i < 50; i++) {
//            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), "hello world-" + i));
//        }



//        PRODUCER.close();
    }

    public static void produceMessage(String message){
        PRODUCER.send(new ProducerRecord<String, String>(TOPIC, message), new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if (metadata != null) {
                    System.out.println(metadata.partition() + "---" + metadata.offset());
                }
            }
        });
    }
}
