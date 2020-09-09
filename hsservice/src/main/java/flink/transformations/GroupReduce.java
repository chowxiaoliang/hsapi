package flink.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * GroupReduce on Grouped DataSet（分组数据集上的 GroupReduce）
 * 应用于分组数据集的 GroupReduce 转换为每个组调用用户定义的 group-reduce 函数。
 * 此与 Reduce 的区别在于，用户定义的函数可一次获取整个组。该函数在组的所有元素上使用 Iterable 调用，并且可以返回任意数量的结果元素。
 */
public class GroupReduce {

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
        dataSet.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer,String>, Tuple2<Integer, String>>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                for (Tuple2<Integer, String> integerStringTuple2 : iterable) {
                    if (integerStringTuple2.f0 % 2 == 0){
                        collector.collect(integerStringTuple2);
                    }
                }
            }
        }).print();
    }
}
