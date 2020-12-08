package spark.core.daily;

import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.Iterator;

public class T2020120801 {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("T20201208").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext();
        String path = "D:\\bigdata\\wordcount.txt";
        JavaRDD<String> javaRDD = javaSparkContext.textFile(path, 3);
        JavaRDD<String> flatMapRdd = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairRDD = flatMapRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });
//        pairRDD.partitionBy(new HashPartitioner(3));
//        RangePartitioner rangePartitioner = new RangePartitioner<scala.math.Ordering.Int$.MODULE$ , scala.reflect.ClassTag$.MODULE$.apply(Integer.class)>(4, pairRDD, true);
//        pairRDD.partitionBy(new RangePartitioner(4, pairRDD, true));

    }
}
