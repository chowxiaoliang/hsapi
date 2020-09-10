package service.project.report;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * 风险地图（窗口的大小和间隔都是5min,风险即为拒绝的案件）
 * 利用flink的滚动窗口功能
 */
public class RiskAndUserMap {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "F_T";
        Properties properties = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        properties.put("bootstrap.servers", "192.168.11.200:9092");
        // 制定consumer group
        properties.put("group.id", "risk");
        // 是否自动确认offset
        properties.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));

        InetSocketAddress inetSocketAddress = new InetSocketAddress("192.168.11.200", 6374);
        Set<InetSocketAddress> set = new HashSet<>();
        set.add(inetSocketAddress);
        FlinkJedisClusterConfig flinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(set).build();

//        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
//        genericObjectPoolConfig.setMaxIdle(flinkJedisClusterConfig.getMaxIdle());
//        genericObjectPoolConfig.setMaxTotal(flinkJedisClusterConfig.getMaxTotal());
//        genericObjectPoolConfig.setMinIdle(flinkJedisClusterConfig.getMinIdle());
//
//        JedisCluster jedisCluster = new JedisCluster(flinkJedisClusterConfig.getNodes(), flinkJedisClusterConfig.getConnectionTimeout(),
//                flinkJedisClusterConfig.getMaxRedirections(), genericObjectPoolConfig);
//        int time = 1;
//        if (jedisCluster.get("time") != null){
//            time = Integer.parseInt(jedisCluster.get("time"));
//        }
//        if(time > 5){
//            time = 1;
//        }
//        System.out.println("获取到的时间间隔是：" + time);

        DataStream<Tuple2<String, Integer>> dataStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for(String letter : s.split(" ")){

                    collector.collect(new Tuple2<>(letter, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.minutes(5), Time.minutes(5)).sum(1);
//        dataStream.print();
        // 同样的效果(打印数据)
//        dataStream.addSink(new PrintSinkFunction<>());
        dataStream.addSink(new RedisSink<>(flinkJedisClusterConfig, new myRedisMapper()));
        streamExecutionEnvironment.execute("test");

    }

    private static class myRedisMapper implements RedisMapper<Tuple2<String, Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "flink");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            System.out.println("key=" + stringIntegerTuple2.f0);
            stringIntegerTuple2.setField(stringIntegerTuple2.f0 + stringIntegerTuple2.f1, 1);
            return stringIntegerTuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            System.out.println("value=" + String.valueOf(stringIntegerTuple2.f1));
            return String.valueOf(stringIntegerTuple2.f1);
        }
    }
}
