package spark.core.variable;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorContext;
import org.apache.spark.util.AccumulatorMetadata;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AccumulatorT {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("AccumulatorT");
        SparkContext sparkContext = new SparkContext(sparkConf);

        String path = "D:\\bigdata\\wordcount.txt";
        JavaRDD<String> javaRDD = sparkContext.textFile(path, 3).toJavaRDD();
        JavaRDD<String> strRdd = javaRDD.flatMap((FlatMapFunction<String, String>) s -> {
            String[] strings = s.split(",");
            return Arrays.asList(strings).iterator();
        });
        LongAccumulator longAccumulator = sparkContext.longAccumulator("longAccumulator");
        List<String> resultList = strRdd.collect();
        for(String result : resultList){
            System.out.println(result);
            longAccumulator.add(1);
        }

        System.out.println("====累加器输出的值是====" + longAccumulator.value());

    }
}
