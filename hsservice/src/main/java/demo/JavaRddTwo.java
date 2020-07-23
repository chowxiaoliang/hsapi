package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JavaRddTwo {
    public static void main(String[] args) {
        System.out.println("==========================");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]").setAppName("JavaRddTwo");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRdd = javaSparkContext.parallelize(Arrays.asList("1","2","3","4","5","5","6"));
        // distinct
//        JavaRDD<String> rdd = javaRdd.distinct();

        // filter
//        JavaRDD<String> rdd = javaRdd.filter("5"::equals);

        // sortBy 根据给定的函数计算key，然后根据这个key来排序value，ascending 为是否升序排列数据
        // sortByKey 是pairRdd特有的函数
//        JavaRDD<String> rdd = javaRdd.sortBy(x -> Integer.parseInt(x)%2, true, 1);

        // 在linux环境下执行linux命令
//        JavaRDD<String> rdd = javaRdd.pipe("head -n 1");

        JavaRDD<String> rdd = javaRdd.
        List<String> result = rdd.collect();
        result.forEach(System.out::println);

    }
}
