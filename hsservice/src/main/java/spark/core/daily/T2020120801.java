package spark.core.daily;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag;
import spark.core.JavaRddT;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class T2020120801 {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("T20201208").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
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
        System.out.println(javaRDD.getNumPartitions());
        System.out.println(javaRDD.partitions().size());
        for(Partition partition : javaRDD.partitions()){
            System.out.println(partition.index());
            System.out.println(partition);
        }
        JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10), 2);
        List<Partition> partitionList = integerJavaRDD.partitions();
        for(Partition partition : partitionList){
            System.out.println(partition.index());
        }
        integerJavaRDD.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> integerIterator) throws Exception {
                if (integerIterator.hasNext()){
                    System.out.println(integerIterator.next());
                }

            }
        });

//        pairRDD.partitionBy(new HashPartitioner(3));
//        RangePartitioner rangePartitioner = new RangePartitioner<scala.math.Ordering.Int$.MODULE$ , scala.reflect.ClassTag$.MODULE$.apply(Integer.class)>(4, pairRDD, true);
//        pairRDD.partitionBy(new RangePartitioner(4, pairRDD, true));


    }
}
