package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(",");

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("javaWordCount").setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        String filePath = "D:\\bigdata\\wordcount.txt";

        JavaRDD<String> lines = javaSparkContext.textFile(filePath);

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        words.foreach(x -> {
            System.out.println(x.intern());
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
//        JavaRDD<Tuple2<String, Integer>> twos = words.map(s -> new Tuple2<>(s, 1));
//        twos.foreach(x -> {
//            System.out.println(x._1 +"_"+ x._2);
//        });
//        Tuple2<String, Integer> resultTuple = twos.reduce((t1, t2) -> {
//            if(t1._1.equals(t2._1)){
//                return new Tuple2<>(t1._1, t1._2 + t2._2);
//            }
//            return null;
//        });
//        System.out.println("======================tuple==========");
//        System.out.println(resultTuple._1);
//        System.out.println(resultTuple._2);
//        System.out.println("======================end============");
        String[] strings = new String[3];
        strings[0] = "zhouliang";

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        javaSparkContext.stop();
    }
}
