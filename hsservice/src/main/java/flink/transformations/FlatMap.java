package flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * flatMap 将集合元素摊开
 */
public class FlatMap {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6));

        // flatMap 将集合元素摊开
        DataSet<Integer> resultSet = dataSet.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
                collector.collect(integer);
            }
        });
        resultSet.print();
    }
}
