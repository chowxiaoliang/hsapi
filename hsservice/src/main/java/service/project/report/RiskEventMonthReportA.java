package service.project.report;

import net.iharder.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import javax.print.attribute.standard.MediaSize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 月报表（sparkSql直接读取hbase的方式）
 */
public class RiskEventMonthReportA {

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RiskEventMonthReportA").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop001,hadoop002,hadoop003");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        String tableName = "RISK_EVENT";
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());
        configuration.set(TableInputFormat.SCAN, scanToString);
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRDD = sparkSession.sparkContext().newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class).toJavaRDD();
        JavaRDD<Row> result = javaRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                Result result = v1._2();
                String ID = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("ID")));
                String PARTNER_ID = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("PARTNER_ID")));
                String RISK_FLOW_NO = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("RISK_FLOW_NO")));
                String EVENT_TYPE = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("EVENT_TYPE")));
                String CERT_NO = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("CERT_NO")));
                String NAME = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("NAME")));
                String MOBILE = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("MOBILE")));
                String BANK_CARD = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("BANK_CARD")));
                String COUNTRY = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("COUNTRY")));
                String PROVINCE = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("PROVINCE")));
                String CITY = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("CITY")));
                String OCCUR_TIME_TO_STR = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("OCCUR_TIME_TO_STR")));
                long OCCUR_TIME_LONG = Bytes.toLong(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("OCCUR_TIME_LONG")));
                String OCCUR_TIME_STR = Bytes.toString(result.getValue(Bytes.toBytes("BASE_INFO"), Bytes.toBytes("OCCUR_TIME_STR")));

                String MAC = Bytes.toString(result.getValue(Bytes.toBytes("DEVICE_INFO"), Bytes.toBytes("MAC")));
                String IMEI = Bytes.toString(result.getValue(Bytes.toBytes("DEVICE_INFO"), Bytes.toBytes("IMEI")));
                String PHONE_OPERATOR = Bytes.toString(result.getValue(Bytes.toBytes("DEVICE_INFO"), Bytes.toBytes("PHONE_OPERATOR")));
                String OS = Bytes.toString(result.getValue(Bytes.toBytes("DEVICE_INFO"), Bytes.toBytes("OS")));

                String DELIVER_ADDRESS_COUNTRY = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("DELIVER_ADDRESS_COUNTRY")));
                String DELIVER_ADDRESS_PROVINCE = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("DELIVER_ADDRESS_PROVINCE")));
                String DELIVER_ADDRESS_CITY = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("DELIVER_ADDRESS_CITY")));
                String DELIVER_ZIP_CODE = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("DELIVER_ZIP_CODE")));
                String DELIVER_MOBILE_NO = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("DELIVER_MOBILE_NO")));
                String DELIVER_NAME = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("DELIVER_NAME")));
                String BANK_NAME = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("BANK_NAME")));
                String BANK_CARD_NO = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("BANK_CARD_NO")));
                String ORIG_ORDER_ID = Bytes.toString(result.getValue(Bytes.toBytes("DELIVER_INFO"), Bytes.toBytes("ORIG_ORDER_ID")));

                String PAYEE_NAME = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_NAME")));
                String PAYEE_EMAIL = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_EMAIL")));
                String PAYEE_MOBILE = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_MOBILE")));
                String PAYEE_ID_NUMBER = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_ID_NUMBER")));
                String PAYEE_CARD_NUMBER = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_CARD_NUMBER")));
                String PAYEE_ID = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_ID")));
                String PAYEE_METHOD = Bytes.toString(result.getValue(Bytes.toBytes("SELLER_INFO"), Bytes.toBytes("PAYEE_METHOD")));

                return RowFactory.create(ID, PARTNER_ID, RISK_FLOW_NO, EVENT_TYPE, CERT_NO, NAME, MOBILE, BANK_CARD, COUNTRY, PROVINCE, CITY, OCCUR_TIME_TO_STR,
                        OCCUR_TIME_LONG, OCCUR_TIME_STR, MAC, IMEI, PHONE_OPERATOR, OS, DELIVER_ADDRESS_COUNTRY, DELIVER_ADDRESS_PROVINCE, DELIVER_ADDRESS_CITY,
                        DELIVER_ZIP_CODE, DELIVER_MOBILE_NO, DELIVER_NAME, BANK_NAME, BANK_CARD_NO, ORIG_ORDER_ID, PAYEE_NAME, PAYEE_EMAIL, PAYEE_MOBILE,
                        PAYEE_ID_NUMBER, PAYEE_CARD_NUMBER, PAYEE_ID, PAYEE_METHOD);
            }
        });

        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("ID", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PARTNER_ID", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("RISK_FLOW_NO", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("EVENT_TYPE", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("CERT_NO", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("NAME", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("MOBILE", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("BANK_CARD", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("COUNTRY", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PROVINCE", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("CITY", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("OCCUR_TIME_TO_STR", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("OCCUR_TIME_LONG", DataTypes.LongType, true));
        structFieldList.add(DataTypes.createStructField("OCCUR_TIME_STR", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("MAC", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("IMEI", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PHONE_OPERATOR", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("OS", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("DELIVER_ADDRESS_COUNTRY", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("DELIVER_ADDRESS_PROVINCE", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("DELIVER_ADDRESS_CITY", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("DELIVER_ZIP_CODE", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("DELIVER_MOBILE_NO", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("DELIVER_NAME", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("BANK_NAME", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("BANK_CARD_NO", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("ORIG_ORDER_ID", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_NAME", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_EMAIL", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_MOBILE", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_ID_NUMBER", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_CARD_NUMBER", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_ID", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("PAYEE_METHOD", DataTypes.StringType, true));

        StructType structType = DataTypes.createStructType(structFieldList);
        Dataset<Row> resultSet = sparkSession.createDataFrame(result, structType);
        resultSet.createOrReplaceTempView("risk_event");
        resultSet.printSchema();
        Dataset<Row> dataset = sparkSession.sql("SELECT PARTNER_ID, EVENT_TYPE,count(*) as sum FROM RISK_EVENT_HIVE WHERE OCCUR_TIME_STR >= '2020-06-01 00:00:00' and OCCUR_TIME_STR <= '2020-06-30 59:59:59' GROUP BY PARTNER_ID , EVENT_TYPE");
        List<Row> resultList = dataset.collectAsList();
        for(Row row : resultList){
            System.out.print("PARTNER_ID = " + row.getString(0));
            System.out.print(" ,EVENT_TYPE = " + row.getString(1));
            System.out.print(" ,sum = " + row.getInt(2));
            System.out.println("================================");
        }
    }
}
