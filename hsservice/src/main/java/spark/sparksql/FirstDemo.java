package spark.sparksql;

import com.google.inject.internal.asm.$ClassAdapter;
import com.google.inject.internal.cglib.core.$DuplicatesPredicate;
import demo.JavaRddT;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

import java.util.ArrayList;
import java.util.List;

public class FirstDemo {

    public static void main(String[] args) throws AnalysisException {

        SparkSession sparkSession = SparkSession.builder().appName("firstDemo").master("local[*]").getOrCreate();

//        runBasicDataFrameExample(sparkSession);
//        runProgrammaticSchemaExample(sparkSession);
        runProgrammaticSchemaExampleA(sparkSession);
    }

    private static void runBasicDataFrameExample(SparkSession sparkSession) throws AnalysisException {
        Dataset<Row> dataset = sparkSession.read().json("D:\\projectCode\\hsapi\\hsservice\\src\\main\\resources\\people.json");
        dataset.printSchema();
        dataset.show();
        // api里面可以直接写各种条件
        dataset.select("name").show();

        dataset.createOrReplaceTempView("people");

        Dataset<Row> dataResult = sparkSession.sql("select * from people where age >= 19");
        List<Row> list = dataResult.collectAsList();
        for(Row row : list){
            System.out.println(row.toString());
        }

        dataset.createGlobalTempView("test");
        sparkSession.sql("select * from global_temp.test where age <= 19").show();

        sparkSession.newSession().sql("select * from global_temp.test where age >= 19").show();

    }

    private void runInferSchemaExample(SparkSession sparkSession){
        // to be concluded
        String filePath = "D:\\projectCode\\hsapi\\hsservice\\src\\main\\resources\\people.txt";
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(filePath,4).toJavaRDD();
        JavaRDD<People> javaRDD1 = javaRDD.map(x -> {
            String[] attributes = x.split(",");
            return new People(attributes[0], Integer.parseInt(attributes[1]));
        });
    }

    private static void runProgrammaticSchemaExample(SparkSession sparkSession){
        String filePath = "D:\\projectCode\\hsapi\\hsservice\\src\\main\\resources\\people.txt";
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(filePath, 4).toJavaRDD();

        JavaRDD<Row> rowJavaRDD = javaRDD.map(x -> {
            String[] datas = x.split(",");
            return RowFactory.create(datas[0], Integer.parseInt(datas[1]));
        });

        //构造表结构
        StructField[] fields = new StructField[2];
        fields[0] = new StructField("name", DataTypes.StringType,true, Metadata.empty());
        fields[1] = new StructField("age", DataTypes.IntegerType, true, Metadata.empty());
        StructType structType = new StructType(fields);

        Dataset<Row> dataset = sparkSession.createDataFrame(rowJavaRDD, structType);
        dataset.show();

        dataset.createOrReplaceTempView("people");
        Dataset<Row> resultSet = sparkSession.sql("select * from people where age >= 19");
        List<Row> list = resultSet.collectAsList();
        for(Row row : list){
            System.out.println(row.toString());
        }
        sparkSession.close();
    }

    private static void runProgrammaticSchemaExampleA(SparkSession sparkSession){
        String filePath = "D:\\projectCode\\hsapi\\hsservice\\src\\main\\resources\\people.txt";
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(filePath, 4).toJavaRDD();
        /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         */
        // 将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> rowJavaRDD = javaRDD.map(x -> {
            String[] fs = x.split(",");
            return RowFactory.create(fs[0], Integer.parseInt(fs[1]));
        });

        /**
         * 第二步：动态构造DataFrame的元数据。
         */
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        // 构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataset = sparkSession.createDataFrame(rowJavaRDD, structType);

        dataset.show();
        dataset.createOrReplaceTempView("people");
        Dataset<Row> resultSet = sparkSession.sql("select * from people where age <= 19");
        List<Row> list = resultSet.collectAsList();
        System.out.println("result data is : ");
        for(Row row : list){
            System.out.println(row.toString());
        }
        sparkSession.close();
    }

    class People {
        private String name;
        private int age;

        public People(String name, int age){
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
