package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

        JavaRDD<String> javaRdd = javaSparkContext.parallelize(Arrays.asList("1","2","3","4","5","5"));
        JavaRDD<String> rdd = javaRdd.distinct();

        List<String> result = rdd.collect();
        result.forEach(System.out::println);

    }
}
