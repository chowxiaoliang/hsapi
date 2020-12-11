package spark.core.daily;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 通过reduceByKey理解spark shuffle
 * 搞清楚reduce(func),reduceByKey(func, numPartitions),reduceByKey(partitioner, func)
 * @author zhouliang
 */
public class T2020121101 {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("T2020121101").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> javaRdd = javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,7,6,5), 3);
//        javaRdd.cache();
        System.out.println("current javaRDD num of partitions is " + javaRdd.getNumPartitions());
        int result = javaRdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("result of reduce is " + result);

        // 注意区分mapPartitionsToPair和mapToPair的区别
        JavaPairRDD<Integer, Integer> javaPairRdd = javaRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Integer>, Integer, Integer>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Integer> integerIterator) throws Exception {
                List<Tuple2<Integer, Integer>> innerList = new ArrayList<>();
                while (integerIterator.hasNext()){
                    int value = integerIterator.next();
                    innerList.add(new Tuple2<>(value, value + value));
                }
                return innerList.iterator();
            }
        }, true);
//        JavaPairRDD<Integer, Integer> javaPairRdd = javaRdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
//                return new Tuple2<>(integer, integer + integer);
//            }
//        });

        // 打印变换过后的分区情况
        System.out.println("current data is " + javaPairRdd.glom().collect());

        Tuple2<Integer, Integer> resultTuple2 = javaPairRdd.reduce(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1, v1._2);
            }
        });
        System.out.println("pair rdd reduce 后的数据是 " + resultTuple2);

        // reduceByKey(func)直接传入一个函数则表示分区默认采用HashPartitioner,个数默认为：
        // 1.在sparkConf里面设置了spark.default.parallelism这个参数，则取这个
        // 2.如果没有设置则取输入rdd的partitions的个数
        JavaPairRDD<Integer, Integer> resultPairRdd = javaPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // reduceByKey(partitioner, func)，partitioner可以自定义
        javaPairRdd.reduceByKey(new HashPartitioner(4), new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        });

        // reduceByKey(func, numPartitions)，默认采用hashPartitioner进行分区，分区数量为传入的numPartitions参数
        javaPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        }, 4);

        System.out.println("pair rdd reduce by key 后的数据是 " + resultPairRdd.glom().collect());
    }
}
