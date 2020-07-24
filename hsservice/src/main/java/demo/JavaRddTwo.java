package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
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
        JavaRDD<String> anotherRDD = javaSparkContext.parallelize(Arrays.asList("4","6","7","7","8"));
        // distinct
//        JavaRDD<String> rdd = javaRdd.distinct();

        // filter
//        JavaRDD<String> rdd = javaRdd.filter("5"::equals);

        // sortBy 根据给定的函数计算key，然后根据这个key来排序value，ascending 为是否升序排列数据
        // sortByKey 是pairRdd特有的函数
//        JavaRDD<String> rdd = javaRdd.sortBy(x -> Integer.parseInt(x)%2, true, 1);

        // 在linux环境下执行linux命令
//        JavaRDD<String> rdd = javaRdd.pipe("head -n 1");

        // 排除相同的元素
//        JavaRDD<String> rdd = javaRdd.subtract(anotherRDD);

        // 联合两个rdd，不排除任何元素
//        JavaRDD<String> rdd = javaRdd.union(anotherRDD);

        // 取两个rdd的交集
//        JavaRDD<String> rdd = javaRdd.intersection(anotherRDD);

        // 对rdd重新分区，返回一个新的rdd
        JavaRDD<String> rdd = javaRdd.coalesce(2);
        JavaRDD<String> rdd = javaRdd.repartition(2);
        List<String> result = rdd.collect();
        result.forEach(System.out::println);

    }
}
