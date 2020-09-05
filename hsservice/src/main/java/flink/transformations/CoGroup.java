package flink.transformations;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

/**
 * coGroup  转换共同处理两个数据集的组。两个数据集都分组在一个定义的键上，
 * 并且两个共享相同键的数据集的组一起交给用户定义的 co-group function。如果对于一个特定的键，
 * 只有一个 DataSet 有一个组，则使用该组和一个空组调用共同组功能。协同功能可以分别迭代两个组的元素并返回任意数量的结果元素。
 *
 * 与 Reduce，GroupReduce 和 Join 相似，可以使用不同的键选择方法来定义键。
 */
public class CoGroup {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Integer>> oneSet = executionEnvironment.fromElements(Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("b", 3));
        DataSet<Tuple2<String, Integer>> twoSet = executionEnvironment.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2));
        oneSet.coGroup(twoSet).where(0).equalTo(0)  .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple2<String, Integer>> out) throws Exception {
                Tuple2<String, Integer> t1 = StreamSupport.stream(first.spliterator(), false).reduce((o1, o2) -> {
                    return new Tuple2<String, Integer>(o1.f0, o1.f1 + o2.f1);
                }).orElse(null);
                Tuple2<String, Integer> t2 = StreamSupport.stream(second.spliterator(), false).reduce((o1, o2) -> {
                    return new Tuple2<String, Integer>(o1.f0, o1.f1 + o2.f1);
                }).orElse(null);
                if (t1 != null && t2 != null) {
                    out.collect(new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1));
                }
            }
        }).print();
    }
}
