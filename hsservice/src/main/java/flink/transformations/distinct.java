package flink.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;

/**
 * distinct 去重
 */
public class distinct {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6));
        DataSet<Integer> resultSet = dataSet.distinct();
        resultSet.print();
    }
}
