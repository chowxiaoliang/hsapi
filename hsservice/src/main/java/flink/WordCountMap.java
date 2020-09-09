package flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * wordCount demo
 */
public class WordCountMap {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromElements(1,2,3,4,5,6,5,6,4,2,6);

        DataSet<Tuple2<Integer, Integer>> resultData = dataSet.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        }).groupBy(0).sum(1);
        resultData.print();
    }
}
