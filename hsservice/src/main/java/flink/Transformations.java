package flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Transformations {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6));

        // flatMap 将集合元素摊开
//        DataSet<Integer> resultSet = dataSet.flatMap(new FlatMapFunction<Integer, Integer>() {
//            @Override
//            public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
//                collector.collect(integer);
//            }
//        });

        // map 映射,将一个元素的数据状态映射成其他数据状态
//        DataSet<Tuple2<Integer, Integer>> resultSet = dataSet.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
//                return new Tuple2<>(integer, integer + 1);
//            }
//        });

        // distinct 去重
//        DataSet<Integer> resultSet = dataSet.distinct();

        // groupBy现根据二元组里面的某个字段分组,aggregate再根据二元组里面的某个字段聚合
//        DataSet<Tuple2<Integer, Integer>> tuple2DataSet = executionEnvironment.fromElements(
//                Tuple2.of(1,2),
//                Tuple2.of(2,2),
//                Tuple2.of(5,2),
//                Tuple2.of(3,4),
//                Tuple2.of(3,6),
//                Tuple2.of(7,5),
//                Tuple2.of(7,15),
//                Tuple2.of(8,3));
//        DataSet<Tuple2<Integer, Integer>> resultSet1 = tuple2DataSet.groupBy(0).aggregate(Aggregations.MAX, 0);
//        DataSet<Tuple2<Integer, Integer>> resultSet2 = tuple2DataSet.groupBy(0).aggregate(Aggregations.MAX, 1);
//        DataSet<Tuple2<Integer, Integer>> resultSet3 = tuple2DataSet.groupBy(1).aggregate(Aggregations.MAX, 0);
//        DataSet<Tuple2<Integer, Integer>> resultSet4 = tuple2DataSet.groupBy(1).aggregate(Aggregations.MAX, 1);
//        System.out.println("dataSet1:");
//        resultSet1.print();
//        System.out.println("dataSet2:");
//        resultSet2.print();
//        System.out.println("dataSet3:");
//        resultSet3.print();
//        System.out.println("dataSet4:");
//        resultSet4.print();

        // filter 根据条件过滤, 返回为true则为过滤留下来的
//        DataSet<Integer> resultSet = dataSet.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer integer) throws Exception {
//                if(integer < 5){
//                    return false;
//                }
//                return true;
//            }
//        });

        // union 联合两个rdd，不排除任何元素
//        DataSet<Integer> anotherSet = executionEnvironment.fromCollection(Arrays.asList(6,7,8,9,10));
//        DataSet<Integer> resultSet = dataSet.union(anotherSet);

        DataSet<Integer> anotherSet = executionEnvironment.fromCollection(Arrays.asList(6,7,8,9,10));
        DataSet<Integer> resultSet = dataSet.cross(anotherSet);
        resultSet.print();
    }
}
