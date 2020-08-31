package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

        // 对rdd重新分区，但是不进行shuffle
//        JavaRDD<String> rdd = javaRdd.coalesce(2);

        // 对rdd重新分区，true表示进行shuffle
//        JavaRDD<String> rdd = javaRdd.coalesce(2, true);

        // 底层默认调用的是coalesce(2, true)，上一个例子
//        JavaRDD<String> rdd = javaRdd.repartition(2);

        // flatMap接收一个变量，返回一个iterator
//        JavaRDD<String> rdd = javaRdd.flatMap(x -> Collections.singletonList(x).iterator());

        // map接收一个变量，返回一个变量
//        JavaRDD<Tuple2<Integer, Integer>> rdd = javaRdd.map(x -> new Tuple2<>(Integer.parseInt(x), Integer.parseInt(x)));
//        List<Tuple2<Integer, Integer>> result = rdd.collect();

        // mapToPair接收一个变量，返回一个二元组
//        JavaPairRDD<Integer, Integer> rdd = javaRdd.mapToPair(x -> new Tuple2<>(Integer.parseInt(x), Integer.parseInt(x)));
//        List<Tuple2<Integer, Integer>> result = rdd.collect();

        // 根据序号生成键值对rdd
        JavaPairRDD<String, Long> rdd = javaRdd.zipWithIndex();
        List<Tuple2<String, Long>> result = rdd.collect();
//        List<String> result = rdd.collect();
        result.forEach(System.out::println);

    }
}
