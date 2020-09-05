package flink.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate on Grouped Tuple DataSet（在分组元组数据集聚合）
 * 有一些常用的聚合操作。聚合转换提供以下内置聚合功能：
 * sum max min
 */
public class Aggregate {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>(1, 1));
        data.add(new Tuple2<>(1, 2));
        data.add(new Tuple2<>(1, 3));
        data.add(new Tuple2<>(2, 4));
        data.add(new Tuple2<>(2, 5));
        data.add(new Tuple2<>(2, 6));
        data.add(new Tuple2<>(3, 7));
        data.add(new Tuple2<>(3, 8));
        data.add(new Tuple2<>(4, 9));
        data.add(new Tuple2<>(4, 10));
        data.add(new Tuple2<>(4, 11));
        data.add(new Tuple2<>(4, 12));
        data.add(new Tuple2<>(5, 13));
        data.add(new Tuple2<>(5, 14));
        data.add(new Tuple2<>(5, 15));
        data.add(new Tuple2<>(5, 16));
        data.add(new Tuple2<>(5, 17));
        data.add(new Tuple2<>(6, 18));
        data.add(new Tuple2<>(6, 19));
        data.add(new Tuple2<>(6, 20));
        data.add(new Tuple2<>(6, 21));

        DataSet<Tuple2<Integer, Integer>> dataSet = executionEnvironment.fromCollection(data);
//        dataSet.groupBy(0).aggregate(Aggregations.SUM, 1).print();

        // MinBy / MaxBy on Grouped Tuple DataSet（分组元组数据集上的 MinBy / MaxBy）MinBy（MaxBy）转换为每个元组组选择一个元组。
        // 选定的元组是其一个或多个指定字段的值最小（最大）的元组。用于比较的字段必须是有效的关键字段，即可比较的字段。
        // 如果多个元组具有最小（最大）字段值，则返回这些元组中的任意元组。
        dataSet.groupBy(0).minBy(1).print();
    }
}
