package flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {

    private static int index = 0;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = streamExecutionEnvironment.socketTextStream("192.168.11.120", 9000, "\n");
        DataStream<String> result = dataStream.map(x -> (index ++) + "input data is :" + x );
        result.print();
        streamExecutionEnvironment.execute();
    }
}
