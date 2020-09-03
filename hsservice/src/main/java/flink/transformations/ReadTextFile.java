package flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 从hdfs或本地文件系统读取文件
 */
public class ReadTextFile {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet = executionEnvironment.readTextFile("E:\\myproject\\bigdata\\flink\\wrodCount\\wordcount.txt");
        dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for(String word : line.split(",")){
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
