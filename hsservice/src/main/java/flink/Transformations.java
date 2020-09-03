package flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;

/**
 * flink 相关的算子 参考:https://www.jianshu.com/p/a0bb414b4c81
 *                      https://www.jianshu.com/p/6a726f2f7137
 */
public class Transformations {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> dataSet = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6));

        // flatMap 将集合元素摊开
//        DataSet<Integer> resultSet = dataSet.flatMap(new FlatMapFunction<Integer, Integer>() {
//            @Override
//            public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
//                collector.collect(integer);
//            }
//        });



        // distinct 去重
//        DataSet<Integer> resultSet = dataSet.distinct();

        // groupBy先根据二元组里面的某个字段分组,aggregate再根据二元组里面的某个字段聚合
//        DataSet<Tuple2<Integer, Integer>> tuple2DataSet = executionEnvironment.fromElements(
//                Tuple2.of(1,2),
//                Tuple2.of(2,2),
//                Tuple2.of(5,2),
//                Tuple2.of(3,4),
//                Tuple2.of(3,6),
//                Tuple2.of(7,5),
//                Tuple2.of(7,15),
//                Tuple2.of(8,3));
//        DataSet<Tuple2<Integer, Integer>> resultSet1 = tuple2DataSet.groupBy(0).aggregate(Aggregations.MAX, 0);
//        DataSet<Tuple2<Integer, Integer>> resultSet2 = tuple2DataSet.groupBy(0).aggregate(Aggregations.MAX, 1);
//        DataSet<Tuple2<Integer, Integer>> resultSet3 = tuple2DataSet.groupBy(1).aggregate(Aggregations.MAX, 0);
//        DataSet<Tuple2<Integer, Integer>> resultSet4 = tuple2DataSet.groupBy(1).aggregate(Aggregations.MAX, 1);
//        System.out.println("dataSet1:");
//        resultSet1.print();
//        System.out.println("dataSet2:");
//        resultSet2.print();
//        System.out.println("dataSet3:");
//        resultSet3.print();
//        System.out.println("dataSet4:");
//        resultSet4.print();

        // filter 根据条件过滤, 返回为true则为过滤留下来的
//        DataSet<Integer> resultSet = dataSet.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer integer) throws Exception {
//                if(integer < 5){
//                    return false;
//                }
//                return true;
//            }
//        });

        // Union 联合两个rdd，不排除任何元素
//        DataSet<Integer> anotherSet = executionEnvironment.fromCollection(Arrays.asList(6,7,8,9,10));
//        DataSet<Integer> resultSet = dataSet.union(anotherSet);

        // coGroup  转换共同处理两个数据集的组。两个数据集都分组在一个定义的键上，并且两个共享相同键的数据集的组一起交给用户定义的 co-group function。如果对于一个特定的键，只有一个 DataSet 有一个组，则使用该组和一个空组调用共同组功能。协同功能可以分别迭代两个组的元素并返回任意数量的结果元素。
//        DataSet<Tuple2<String, Integer>> oneSet = executionEnvironment.fromElements(Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("b", 3));
//        DataSet<Tuple2<String, Integer>> twoSet = executionEnvironment.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2));
//        oneSet.coGroup(twoSet).where(0).equalTo(0)  .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple2<String, Integer>> out) throws Exception {
//                Tuple2<String, Integer> t1 = StreamSupport.stream(first.spliterator(), false).reduce((o1, o2) -> {
//                    return new Tuple2<String, Integer>(o1.f0, o1.f1 + o2.f1);
//                }).orElse(null);
//                Tuple2<String, Integer> t2 = StreamSupport.stream(second.spliterator(), false).reduce((o1, o2) -> {
//                    return new Tuple2<String, Integer>(o1.f0, o1.f1 + o2.f1);
//                }).orElse(null);
//                if (t1 != null && t2 != null) {
//                    out.collect(new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1));
//                }
//            }
//        }).print();

        // cross 转换将两个数据集组合为一个数据集。它构建两个输入数据集的元素的所有成对组合，即构建笛卡尔积
//        DataSet<Tuple2<Integer, String>> oneSet = executionEnvironment.fromElements(Tuple2.of(1,"a"), Tuple2.of(2,"b"), Tuple2.of(3," c"));
//        DataSet<Tuple2<Integer, String>> twoSet = executionEnvironment.fromElements(Tuple2.of(1,"a"), Tuple2.of(1,"b"));
//        oneSet.cross(twoSet).with(new CrossFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>() {
//            @Override
//            public Tuple2<Integer, String> cross(Tuple2<Integer, String> integerStringTuple2, Tuple2<Integer, String> integerStringTuple22) throws Exception {
//                return new Tuple2<>(integerStringTuple2.f0, integerStringTuple22.f1);
//            }
//        }).print();


    }

}
