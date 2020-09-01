package flink.transformations;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MapPartition {

    public static void main(String[] args) {

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

        dataSet.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {

            }
        })
    }
}
