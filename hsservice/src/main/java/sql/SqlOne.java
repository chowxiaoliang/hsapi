package sql;

import org.apache.avro.ipc.DatagramServer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;

public class SqlOne {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SqlOne").setMaster("local[3]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password","QA@skyroam");
        properties.setProperty("driver","com.mysql.jdbc.Driver");
        Dataset<Row> dataset = sparkSession.read().jdbc("jdbc:mysql://192.168.9.42:3306/engine", "engine_rule", properties);

        dataset.createOrReplaceTempView("t_rule");
//      type DataFrame = Data<Row>
        Dataset<Row> dataResult = sparkSession.sql("select * from t_rule");
        List<Row> list = dataResult.collectAsList();
        for(Row row: list){
            System.out.println(row.toString());
        }

        sparkSession.stop();
    }
}
