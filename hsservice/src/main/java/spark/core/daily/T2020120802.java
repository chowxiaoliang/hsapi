package spark.core.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.collection.TraversableOnce;

public class T2020120802 {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("T2020120802").setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(sparkConf);
        String path = "D:\\bigdata\\wordcount.txt";
        RDD<String> rdd = sparkContext.textFile(path, 4);

    }
}
