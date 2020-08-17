package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 加载或创建源数据
        DataSet<String> text = env.fromElements("this a book", "i love china", "i am chinese");

        // 转化处理数据
        DataSet<Tuple2<String, Integer>> ds = text.flatMap(new LineSplitter()).groupBy(0).sum(1);

        // 输出数据到目的端
        ds.print();

        // 执行任务操作
        // 由于是Batch操作，当DataSet调用print方法时，源码内部已经调用Excute方法，所以此处不再调用，如果调用会出现错误
        //env.execute("Flink Batch Word Count By Java");

    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word:line.split(" ")) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
