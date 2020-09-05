package flink.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Reduce on Grouped DataSet（减少分组数据集）
 * 应用于分组数据集的 Reduce 转换使用用户定义的 reduce 函数将每个组简化为单个元素。
 * 对于每组输入元素，reduce 函数依次将成对的元素组合为一个元素，直到每组只剩下一个元素为止。
 */
public class Reduce {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> data = new ArrayList<>();

        data.add(new Tuple2<>(1, "demo1"));
        data.add(new Tuple2<>(1, "demo2"));
        data.add(new Tuple2<>(1, "demo3"));
        data.add(new Tuple2<>(2, "demo4"));
        data.add(new Tuple2<>(2, "demo5"));
        data.add(new Tuple2<>(2, "demo6"));
        data.add(new Tuple2<>(3, "demo7"));
        data.add(new Tuple2<>(3, "demo8"));
        data.add(new Tuple2<>(4, "demo9"));
        data.add(new Tuple2<>(4, "demo10"));
        data.add(new Tuple2<>(4, "demo11"));
        data.add(new Tuple2<>(4, "demo12"));
        data.add(new Tuple2<>(5, "demo13"));
        data.add(new Tuple2<>(5, "demo14"));
        data.add(new Tuple2<>(5, "demo15"));
        data.add(new Tuple2<>(5, "demo16"));
        data.add(new Tuple2<>(5, "demo17"));
        data.add(new Tuple2<>(6, "demo18"));
        data.add(new Tuple2<>(6, "demo19"));
        data.add(new Tuple2<>(6, "demo20"));
        data.add(new Tuple2<>(6, "demo21"));

        DataSet<Tuple2<Integer, String>> dataSet = executionEnvironment.fromCollection(data);
        dataSet.groupBy(0).reduce(new ReduceFunction<Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> reduce(Tuple2<Integer, String> integerStringTuple2, Tuple2<Integer, String> t1) throws Exception {
                return new Tuple2<>(integerStringTuple2.f0, integerStringTuple2.f1 + t1.f1);
            }
        }).print();
    }
}
