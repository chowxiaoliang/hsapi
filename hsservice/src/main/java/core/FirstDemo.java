package core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class FirstDemo {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaSparkContext javaSparkContextt = new JavaSparkContext();

        JavaSparkContext javaSparkContext = new JavaSparkContext("123", "123");

        JavaRDD<String> rddFile = sparkContext.textFile("hdfs://");
        JavaRDD<String> lineFile = rddFile.filter(line -> line.contains("text"));

        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("1","2","3"));
    }
}
