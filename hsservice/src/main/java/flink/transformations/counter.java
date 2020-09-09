package flink.transformations;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 计数器
 */
public class counter {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet = executionEnvironment.fromElements("who is there", "i think no body!");

        dataSet.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            IntCounter intCounter = new IntCounter(0);
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String letter : s.split(" ")){
                    collector.collect(new Tuple2<>(letter, 1));
                    intCounter.add(1);
                }
            }
            @Override
            public void open(Configuration configuration){
                getRuntimeContext().addAccumulator("c", intCounter);
            }
        }).setParallelism(4).writeAsText("E:\\myproject\\bigdata\\flink\\counter");

        JobExecutionResult jobExecutionResult = executionEnvironment.execute("counter");
        Object c = jobExecutionResult.getAccumulatorResult("c");
        System.out.println("c=" + c);
    }
}
