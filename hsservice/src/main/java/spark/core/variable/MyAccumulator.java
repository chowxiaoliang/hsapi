package spark.core.variable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigInteger;
import java.util.List;

public class MyAccumulator {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("MyAccumulator").setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(sparkConf);
        SelfDeAccumulator.BigIntegerAccumulator selfDeAccumulator = new SelfDeAccumulator.BigIntegerAccumulator();
        sparkContext.register(selfDeAccumulator);

        String path = "D:\\bigdata\\wordcount.txt";
        JavaRDD<String> javaRDD = sparkContext.textFile(path, 3).toJavaRDD();
        List<String> resultList = javaRDD.collect();
        for(String string : resultList){
            System.out.println(string);
            selfDeAccumulator.add(BigInteger.ONE);
        }
        System.out.println("my accumulator is " + selfDeAccumulator.value());
    }
}
