package spark.sparksql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadDataFromHbase {

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("ReadDataFromHbase");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop001,hadoop002,hadoop003");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Scan scan = new Scan();
        String tableName = "RISK_EVENT";
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());
        configuration.set(TableInputFormat.SCAN, scanToString);
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRDD = sparkSession.sparkContext().newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class).toJavaRDD();
        JavaRDD<Row> riskEventRdd = javaRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>,Row>() {
            @Override
            public Row call(Tuple2<ImmutableBytesWritable,Result> value) throws Exception {
                Result result = value._2();
                String rowkey = Bytes.toString(result.getRow());
                String id = Bytes.toString(result.getValue(Bytes.toBytes("INFO"), Bytes.toBytes("id")));
                String name = Bytes.toString(result.getValue(Bytes.toBytes("INFO"), Bytes.toBytes("name")));
                String password = Bytes.toString(result.getValue(Bytes.toBytes("INFO"), Bytes.toBytes("password")));

                //这一点可以直接转化为row类型
                return RowFactory.create(rowkey,id,name,password);
            }
        });

        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("password", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFieldList);

        Dataset<Row> dataset = sparkSession.createDataFrame(riskEventRdd, structType);
        dataset.printSchema();
        dataset.createOrReplaceTempView("risk_event");
        Dataset<Row> result = sparkSession.sql("select * from risk_event");
    }
}
