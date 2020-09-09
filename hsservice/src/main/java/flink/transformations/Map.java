package flink.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

/**
 * map 映射,将一个元素的数据状态映射成其他数据状态
 */
public class Map {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6));

        DataSet<Tuple2<Integer, Integer>> resultSet = dataSet.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                return new Tuple2<>(integer, integer + 1);
            }
        });
        resultSet.print();
    }
}
