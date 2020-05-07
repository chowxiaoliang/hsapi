package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @desc wordCount
 */
public class WcApp {

    static {
        try {
            System.load("D:\\hadoop\\hadoop-2.10.0\\bin\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[4]");
        sparkConf.setAppName("wc");

        // 创建spark上下文对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 加载数据，创建弹性分布式数据集
        JavaRDD<String> rdd = javaSparkContext.textFile("E:\\myproject\\bigdata\\inputOne\\wordcount-spark.txt");
        JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> {
            String[] strings = s.split(",");
            return Arrays.asList(strings).iterator();
        });

        words.filter(x -> x.contains("name"));

        JavaPairRDD<String, Integer> counts = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> result = counts.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        result.saveAsTextFile("E:\\myproject\\bigdata\\inputOne\\wordcount-spark-result.txt");

    }
}
