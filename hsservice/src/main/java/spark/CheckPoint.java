package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 首先需要用sparkContext设置hdfs的checkpoint的目录,然后创建RDD操作，最后在RDD上调用checkpoint的方法。
 * rdd.cache()原因：在flatMap计算完毕后，checkpoint会再次通过RunJob做一次计算，将每个partition数据保存到HDFS。
 * 这样RDD将会计算两次，所以为了避免此类情况，最好将RDD进行cache。
 */
public class CheckPoint {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("checkPoint").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setCheckpointDir("D:\\bigdata\\checkpointDir");
        JavaRDD<String> lines = sparkContext.textFile("D:\\bigdata\\wordcount.txt");
        JavaRDD<String> javaRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        javaRDD.cache();
        javaRDD.checkpoint();
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        javaPairRDD.checkpoint();
        JavaPairRDD<String, Integer> resultRdd = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        resultRdd.collect();
    }
}
