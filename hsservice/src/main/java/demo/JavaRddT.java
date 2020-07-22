package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import javax.swing.*;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class JavaRddT {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("JavaRddT").setMaster("local[1]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        String path = "D:\\bigdata\\wordcount.txt";
        JavaRDD<String> lines = javaSparkContext.textFile(path, 3);
        JavaRDD<String> line = lines.flatMap(s -> Arrays.asList(s.split(",")).iterator());

        JavaPairRDD<String, Integer> javaPairRdd = line.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairRDD<String, Integer> resultRdd = javaPairRdd.reduceByKey(Integer::sum);
        List<Tuple2<String, Integer>> resultList = resultRdd.collect();
        for(Tuple2<String, Integer> tuple2 : resultList){
            System.out.println(tuple2);
        }
        javaSparkContext.stop();
    }
}
