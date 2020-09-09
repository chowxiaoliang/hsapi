package flink.transformations;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Projection of Tuple DataSet（元组数据集投影）
 * project 转换将删除或移动元组数据集的元组字段。该 project(int…) 方法选择应由其索引保留的元组字段，并在输出元组中定义其顺序。
 * project 不需要定义函数体
 */
public class project {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<Integer, Integer, Integer>> list = new ArrayList<>();
        list.add(new Tuple3<>(1,2,3));
        list.add(new Tuple3<>(4,5,6));
        list.add(new Tuple3<>(7,8,9));
        executionEnvironment.fromCollection(list).project(2,0).print();
    }
}
