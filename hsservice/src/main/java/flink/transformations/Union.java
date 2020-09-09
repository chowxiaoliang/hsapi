package flink.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;

/**
 * Union 联合两个dataSet，不排除任何元素
 */
public class Union {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6));
        DataSet<Integer> anotherSet = executionEnvironment.fromCollection(Arrays.asList(6,7,8,9,10));
        DataSet<Integer> resultSet = dataSet.union(anotherSet);
        resultSet.print();
    }

}
