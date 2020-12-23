package spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * spark流处理，从kafka消费数据
 */
public class KafkaDataSource {

    private static final String HOST = "192.168.11.200:9092";

    private static final String GROUP = "vsim2";

    private static final String TOPIC = "vsim2sync";

    /**
     * 数据分割的规则
     */
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaDataSource").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Map<String, Integer> threadMap = new HashMap<>(1);
        threadMap.put(TOPIC, 1);
        JavaPairInputDStream<String, String> javaPairInputDStream = KafkaUtils.createStream(javaStreamingContext, HOST, GROUP, threadMap);
        // sparkStreaming读取kafka数据的两种方式
        // 1.receiver方式
        JavaPairReceiverInputDStream<String, String> receiverInputDStream = KafkaUtils.createStream(javaStreamingContext, HOST,GROUP,threadMap);
        JavaDStream<String> words = javaPairInputDStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> stringStringTuple2) {
                return Arrays.asList(SPACE.split(stringStringTuple2._2)).iterator();
            }
        });

        Set<String> topicSet = new HashSet<>();
        topicSet.add(TOPIC);
        Map<String, String> kafkaParams = new HashMap<>(10);
        kafkaParams.put("bootstrap.server", "192.168.11.200:9092");
        // 2.直接读取方式
        KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);
        //统计
        JavaPairDStream<String, Integer> result = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1 + v2;
            }
        });

        try {
            result.print();
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
