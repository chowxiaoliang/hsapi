package spark.streaming.daily;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import spark.streaming.KafkaDataSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class T2020122301 {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("T2020122301").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, String> kafkaParameters = new HashMap<String, String>(10);
        kafkaParameters.put("bootstrap.servers", "192.168.9.31:9092");
        // 是否自动确认offset
        kafkaParameters.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        kafkaParameters.put("auto.commit.interval.ms", "1000");
        kafkaParameters.put("group.id","zl");

        HashSet<String> topics = new HashSet<String>();
        topics.add("kfk");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParameters,
                topics);

        lines.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.stop();
//        lines.flatMap(new FlatMapFunction<Tuple2<String, String>, Object>() {
//            @Override
//            public Iterator<Object> call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                return null;
//            }
//        })

    }
}

