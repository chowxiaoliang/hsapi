package flink.transformations;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 分区计算
 */
public class MapPartition {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> tuple2List = new ArrayList<>();

        tuple2List.add(new Tuple2<>(1, "demo1"));
        tuple2List.add(new Tuple2<>(2, "demo2"));
        tuple2List.add(new Tuple2<>(3, "demo3"));
        tuple2List.add(new Tuple2<>(4, "demo4"));
        tuple2List.add(new Tuple2<>(5, "demo5"));
        tuple2List.add(new Tuple2<>(6, "demo6"));
        tuple2List.add(new Tuple2<>(7, "demo7"));

        DataSet<Tuple2<Integer, String>> dataSet = executionEnvironment.fromCollection(tuple2List);

        // 按照数字分区，相同的数据会分到同一个分区里
        System.out.println("按照数字分区！");
        dataSet.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                iterable.forEach(x -> System.out.println("当前线程id是：" + Thread.currentThread().getId() + "," + x));
                System.out.println();
            }
        }).setParallelism(4).print();


        // 按照hash分区
        System.out.println("按照hash分区！");
        dataSet.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Object>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Object> collector) throws Exception {
                iterable.forEach(x -> System.out.println("当前线程id是：" + Thread.currentThread().getId() + "," + x));
                System.out.println();
            }
        }).setParallelism(4).print();

        // 自定义分区
        System.out.println("自定义分区！");
        dataSet.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                System.out.println("key=" + key + ", numPartitions=" + numPartitions);
                return key % numPartitions;
            }
        }, 0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Object>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Object> collector) throws Exception {
                iterable.forEach(x -> System.out.println("当前线程id是：" + Thread.currentThread().getId() + "," + x));
                System.out.println();
            }
        }).setParallelism(4).print();
    }
}
