package spark.streaming.daily;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class T2020122301 {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("T2020122301").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Map<String, String> kafkaParameters = new HashMap<String, String>();
        kafkaParameters.put("metadata.broker.list", "192.168.179.3:9092,192.168.179.4:9092,192.168.179.5:9092");

        HashSet<String> topics = new HashSet<String>();
        topics.add("kfk");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParameters,
                topics);

    }
}

