package flink.transformations;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * cross 转换将两个数据集组合为一个数据集。它构建两个输入数据集的元素的所有成对组合，即构建笛卡尔积
 */
public class Cross {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, String>> oneSet = executionEnvironment.fromElements(Tuple2.of(1,"a"), Tuple2.of(2,"b"), Tuple2.of(3," c"));
        DataSet<Tuple2<Integer, String>> twoSet = executionEnvironment.fromElements(Tuple2.of(1,"a"), Tuple2.of(1,"b"));
        oneSet.cross(twoSet).with(new CrossFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> cross(Tuple2<Integer, String> integerStringTuple2, Tuple2<Integer, String> integerStringTuple22) throws Exception {
                return new Tuple2<>(integerStringTuple2.f0, integerStringTuple22.f1);
            }
        }).print();
    }
}
