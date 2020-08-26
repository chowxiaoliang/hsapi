package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 滑动窗口
 * 两个必要参数：窗口大小，滑动间隔
 *
 * 1.window 对每个滑动窗口的数据执行自定义的操作
 * 2.countByWindow 对每个滑动窗口的数据执行count操作
 * 3.reduceByWindow 对每个滑动窗口的数据执行reduce操作
 * 4.countByKeyAndWindow 对每个滑动窗口的数据执行countByKey操作
 * 5.reduceByKeyAndWindow 对每个滑动窗口的数据执行reduceByKey操作
 */
public class Window {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("window").setMaster("local[*]");
        // 拉去一个batch（批次）的时间间隔
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> javaReceiverInputDStream = javaStreamingContext.socketTextStream("localhost", 9999);

        JavaDStream<String> javaDStream = javaReceiverInputDStream.map( x -> x.split(" ")[0]);
        JavaPairDStream<String, Integer> javaPairDStream = javaDStream.mapToPair(x -> new Tuple2<>(x, 1));

        // 滑动窗口的大小是60s，也就是12个RDD，滑动间隔是20s（每隔20s往前回溯12个RDD进行计算）(每四个批次计算一次窗口的结果)
        JavaPairDStream<String, Integer> resultPairDStream = javaPairDStream.reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) Integer::sum, Durations.seconds(60), Durations.seconds(20));
        resultPairDStream.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.stop();
        javaStreamingContext.close();
    }
}
