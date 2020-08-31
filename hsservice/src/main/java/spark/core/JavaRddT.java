package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JavaRddT {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("JavaRddT").setMaster("local[1]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        String path = "D:\\bigdata\\wordcount.txt";
        JavaRDD<String> lines = javaSparkContext.textFile(path, 3);
        JavaRDD<String> line = lines.flatMap(s -> Arrays.asList(s.split(",")).iterator());

        // 一种写法
        JavaPairRDD<String, Integer> javaPairRdd1 = line.mapToPair(s -> new Tuple2<>(s,1));

        JavaPairRDD<String, Integer> resultRdd1 = javaPairRdd1.reduceByKey(Integer::sum);
        List<Tuple2<String, Integer>> resultList1 = resultRdd1.collect();
        for(Tuple2<String, Integer> tuple2 : resultList1){
            System.out.println(tuple2);
        }
        System.out.println("==========================================");
        // 另一种写法
        JavaRDD<Tuple2<String, Integer>> javaPairRdd2 = line.map(s -> new Tuple2<>(s,1));
        List<Tuple2<String, Integer>> resultList = javaPairRdd2.collect();
        for(Tuple2<String, Integer> tuple2 : resultList){
            System.out.println("value:" + tuple2);
        }
        System.out.println("=============================================");
        // 这种写法只能得到汇总的结果，key为最后选取的那个key
        Tuple2<String, Integer> resultRdd2 = javaPairRdd2.reduce((s1, s2) -> new Tuple2<>(s1._1, s1._2 + s2._2));
        System.out.println("resultRdd1=" + resultRdd2);
        javaSparkContext.stop();
    }
}
