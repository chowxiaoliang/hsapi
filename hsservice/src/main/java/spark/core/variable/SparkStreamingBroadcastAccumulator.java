package spark.core.variable;

import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;

import java.util.List;

/**
 * 使用广播变量和累加器对黑名单变量进行过滤
 * @author zhouliang
 */
public class SparkStreamingBroadcastAccumulator {

    private static volatile Accumulator accumulator;
    private static volatile Broadcast<List<String>> broadcast;

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("SparkStreamingBroadcastAccumulator");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        JavaSparkContext sparkContext = javaStreamingContext.sparkContext();
        accumulator = sparkContext.intAccumulator(0, "t");

        JavaReceiverInputDStream<String> javaReceiverInputDStream = javaStreamingContext.socketTextStream("localhost", 8080);
        javaReceiverInputDStream
    }
}
