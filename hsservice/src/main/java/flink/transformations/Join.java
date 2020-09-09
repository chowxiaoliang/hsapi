package flink.transformations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Join OuterJoin 合并两个数据流 join with
 * 内连接，左外连接，右外连接，全连接
 * 笛卡尔积为算子 cross
 */
public class Join {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, String>> dataSetOne = executionEnvironment.fromElements(new Tuple2<>(1,"小王"),new Tuple2<>(2,"小李"),new Tuple2<>(3,"小张"));
        DataSet<Tuple2<Integer, String>> dataSetTwo = executionEnvironment.fromElements(new Tuple2<>(1,"北京"),new Tuple2<>(2,"上海"),new Tuple2<>(3,"武汉"));

        // 内连接，左外连接，右外连接，全连接
//        dataSetOne.join(dataSetTwo)
//        dataSetOne.leftOuterJoin(dataSetTwo)
//        dataSetOne.rightOuterJoin(dataSetTwo)
        dataSetOne.fullOuterJoin(dataSetTwo)
                .where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer,String,String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(first == null){
                    return new Tuple3<>(second.f0, "-", second.f1);
                }else if (second == null){
                    return new Tuple3<>(first.f0, "-",  first.f1);
                }else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
    }
}
