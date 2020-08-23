package spark.sparksql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import spark.People;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @desc DataSet和RDD不同的地方：
 * DataSet和RDD比较类似,不同的地方就是序列化的方式
    RDD序列化默认是使用Java的serialization或者kryo实现序列化和反序列化的
    DataSet是使用的encoder来实现对象的序列化和在网络中的传输
    encoder有个动态的特性,Spark在执行比如sorting之类的操作时无需再把字节反序列化成对象
 */
public class SparkEncoder {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("sparkEncoder").getOrCreate();
        People people = new People("zhouliang", 10);
        Encoder<People> encoder = Encoders.bean(People.class);

        Dataset<People> dataSet = sparkSession.createDataset(Collections.singletonList(people), encoder);
        dataSet.show();

        // 常见类型的编码器
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> integerDataset = sparkSession.createDataset(Arrays.asList(1,2), integerEncoder);
        Dataset<Integer> resultSet = integerDataset.map((MapFunction<Integer, Integer>) integer -> integer + 1, integerEncoder);
        List<Integer> resultList = resultSet.collectAsList();
        for (Integer integer : resultList) {
            System.out.println(integer);
        }

        String url = "E:\\myproject\\hsapi\\hsservice\\src\\main\\resources\\people.json";
        Dataset<People> peopleDataset = sparkSession.read().json(url).as(encoder);
        peopleDataset.printSchema();
        peopleDataset.show();
    }

}
