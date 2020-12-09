package spark.core.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * map join 和 reduce join
 * 在使用这两种算法处理较大规模的数据时，通常需要对多个参数进行调优，否则可能会产生OOM问题。
 * 通常需要调优的相关参数包括，map端数据输出buffer大小，reduce端数据分组方法（基于map还是基于sort），等等。
 */
public class T2020120901 {


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("T20200901");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> javaRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> javaRDD2 = javaSparkContext.parallelize(Arrays.asList(6,7,8,9,10));
        // 这里会发生shuffle（reduce join）
        JavaRDD<Integer> unionRdd = javaRDD1.union(javaRDD2);



    }
}
