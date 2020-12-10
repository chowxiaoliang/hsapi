package spark.core.daily;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import spark.core.JavaRddT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 分区相关的操作
 * 1.foreach和foreachPartition的区别
 * 2.repartition和partitionBy的区别
 * 3.parallelize和textFile获取数据时的分区情况
 * 4.map，mapPartition和mapPartitionsWithIndex的区别
 */
public class T2020121001 {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("T2020121001");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 创建普通的rdd，3个分区
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        // 获取分区的数量
        int numPartitions = javaRDD.getNumPartitions();
        System.out.println("num of partition is " + numPartitions);
        // 获取分区的下标
        List<Partition> partitionList = javaRDD.partitions();
        for (Partition partition : partitionList) {
            System.out.println("currentPartition is " + partition.index());
        }

        // 普通的rdd转化成pairRdd
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, integer);
            }
        });
        // 转换成pairRDD之后的分区情况
        int pairNumPartitions = javaPairRDD.getNumPartitions();
        System.out.println("num of pairRDD partitions is " + pairNumPartitions);
        // 通过repartition()进行重新分区
        JavaPairRDD<Integer, Integer> repartitionRDD = javaPairRDD.repartition(2);
        int repartitionNumPartitions = repartitionRDD.getNumPartitions();
        System.out.println("num of repartition is " + repartitionNumPartitions);
        System.out.println("repartition rdd is " + repartitionRDD.glom().collect());

        System.out.println("=============通过repartition分区之后元素的分布是===========");
        List<Tuple2<String, List<Tuple2<Integer, Integer>>>> repartitionList
                = repartitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<Tuple2<String, List<Tuple2<Integer, Integer>>>>>() {
            List<Tuple2<String, List<Tuple2<Integer, Integer>>>> repartitionList = new ArrayList<>();
            @Override
            public Iterator<Tuple2<String, List<Tuple2<Integer, Integer>>>> call(Integer v1, Iterator<Tuple2<Integer, Integer>> v2) throws Exception {
                List<Tuple2<Integer, Integer>> innerList = new ArrayList<>();
                while (v2.hasNext()){
                    Tuple2<Integer, Integer> result = v2.next();
                    innerList.add(result);
                }
                repartitionList.add(new Tuple2<>("当前分区是：" + v1, innerList));
                return repartitionList.iterator();
            }
        }, true).collect();
        for (Tuple2<String, List<Tuple2<Integer, Integer>>> tuple2Tuple2 : repartitionList){
            System.out.println(tuple2Tuple2._1 + "," + tuple2Tuple2._2);
        }

        // 通过partitionBy进行重新分区
        JavaPairRDD<Integer, Integer> partitionByRDD = repartitionRDD.partitionBy(new HashPartitioner(2));
        int partitionByRDDPartitions = partitionByRDD.getNumPartitions();
        System.out.println("num of partitionByRDD is " + partitionByRDDPartitions);
        System.out.println("partition by rdd is " + partitionByRDD.glom().collect());

        // 注意搞清楚map和mapPartition的区别
        javaRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Object>() {
            @Override
            public Iterator<Object> call(Iterator<Integer> integerIterator) throws Exception {
                return null;
            }
        });

        // 注意搞清楚foreach和foreachPartition的区别
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {

            }
        });

        javaPairRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {

            }
        });

        // mapPartitionsWithIndex相关的操作
        List<Tuple2<String, Integer>> partRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 1, 1))
                .mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>,
                        Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(Integer v1, Iterator<Integer> v2) throws Exception {

                        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

                        int totalElement = 0;
                        while (v2.hasNext()) {
                            v2.next();
                            totalElement++;
                        }

                        list.add(new Tuple2<String, Integer>("part_" + v1, totalElement));

                        return list.iterator();
                    }
                }, true).collect();

        for (Tuple2<String, Integer> tuple2 : partRDD) {
            System.out.println("分区：" + tuple2._1() + " 数量： " + tuple2._2());
        }

        List<Tuple2<String, List<Integer>>> dataList = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 1, 1))
                .mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>,
                        Iterator<Tuple2<String, List<Integer>>>>() {
                    @Override
                    public Iterator<Tuple2<String, List<Integer>>> call(Integer v1, Iterator<Integer> v2) throws Exception {

                        List<Tuple2<String, List<Integer>>> list = new ArrayList<Tuple2<String, List<Integer>>>();

                        List<Integer> elementList = new ArrayList<Integer>();
                        while (v2.hasNext()) {
                            elementList.add(v2.next());
                        }

                        list.add(new Tuple2<String, List<Integer>>("part_" + v1, elementList));

                        return list.iterator();
                    }
                }, true).collect();


        for (Tuple2<String, List<Integer>> tuple2 : dataList) {
            System.out.println("分区：" + tuple2._1() + " 元素： " + tuple2._2());
        }
    }
}
