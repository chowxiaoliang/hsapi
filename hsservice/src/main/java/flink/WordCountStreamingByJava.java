package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountStreamingByJava {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置socket数据源
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.socketTextStream("localhost", 9999, "\n");
        DataStream<WordWithCount> dataStream = dataStreamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                for(String word : s.split(" ")){
                    collector.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(2)).sum("count");

        dataStream.print();
        // 执行任务操作
        streamExecutionEnvironment.execute("Flink Streaming Word Count By Java");
    }

    public static class WordWithCount{
        private String word;

        private int count;

        public WordWithCount(){}

        public WordWithCount(String word, int count){
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
