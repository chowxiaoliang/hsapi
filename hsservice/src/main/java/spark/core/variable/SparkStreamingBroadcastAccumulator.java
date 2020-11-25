package spark.core.variable;

import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 使用广播变量和累加器对黑名单变量进行过滤
 * @author zhouliang
 */
public class SparkStreamingBroadcastAccumulator {

    private static volatile LongAccumulator longAccumulator;
    private static volatile Broadcast<List<String>> broadcast;

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("SparkStreamingBroadcastAccumulator");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(3));
        SparkContext javaSparkContext = javaStreamingContext.sparkContext().sc();
        javaSparkContext.setLogLevel("error");

        longAccumulator = javaSparkContext.longAccumulator("t");

        broadcast = javaStreamingContext.sparkContext().broadcast(Arrays.asList("zhou", "liang"));
        System.out.println("=========开始监听数据==============");
        System.out.println(" BlackList appeared : " + longAccumulator.value() + " times");
        JavaReceiverInputDStream<String> javaReceiverInputDStream = javaStreamingContext.socketTextStream("localhost", 8080);
        javaReceiverInputDStream.print();

        JavaPairDStream<String, Integer> javaPairDStream = javaReceiverInputDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        javaPairDStream.print();
        JavaPairDStream<String, Integer> javaPairDStream1 = javaPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        javaPairDStream1.print();
        javaPairDStream1.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                stringIntegerJavaPairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                        if (broadcast.value().contains(v1._1)){
                            longAccumulator.add(v1._2);
                            return false;
                        }else {
                            return true;
                        }
                    }
                }).collect();
            }
        });
        System.out.println(" BlackList appeared : " + longAccumulator.value() + " times");
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        javaStreamingContext.stop();
    }
}
