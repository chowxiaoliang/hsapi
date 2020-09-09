package flink.transformations;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * GroupCombine on a Grouped DataSet 可组合的 GroupReduce 函数
 *
 */
public class GroupCombine {

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

        DataSet<Tuple2<Integer, String>> dataset = executionEnvironment.fromCollection(data);
        dataset.groupBy(0).reduceGroup(new MyGroupReduce()).print();

    }

    private static class MyGroupReduce implements GroupCombineFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>, GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

        @Override
        public void combine(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
            Tuple2<Integer, String> t = StreamSupport.stream(iterable.spliterator(), false).reduce((o1, o2) -> {
                return new Tuple2<>(o1.f0, o1.f1 + " + " + o2.f1);
            }).get();
            collector.collect(t);
        }

        @Override
        public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
            iterable.forEach(o->{
                collector.collect(o);
            });
        }
    }
}
