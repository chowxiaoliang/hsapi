package spark.core.serial;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import spark.People;

import java.util.Arrays;

/**
 * Kryo序列化方式
 * 美音 [k'raɪəʊ]
 *
 * Spark中默认使用的是java的序列化机制，JavaSerializer
 * 如果需要使用Kryo序列化，需要进行注册。
 * kryo序列化特点：
 *
 * 速度快，占用资源少。
 *
 */
public class KryoSerial {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KryoSerial").setMaster("local[*]");
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[]{People.class});

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

    }
}
