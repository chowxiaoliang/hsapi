package spark.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark.People;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据不同的数据源创建dataFrame的方式(RDD,dataSet,dataFrame之间的区别，实际就是创建dataFrame)
 * 三个步骤：
 * 1.创建javaRDD
 * 2.创建StructType的schema，与rowJavaRDD结构相匹配
 * 3.通过sparkSession的createDataFrame把schema应用到rowJavaRDD上面形成新的dataFrame
 */
public class DataFrameCreation {

    private final static String FILE_PATH = "E:\\myproject\\hsapi\\hsservice\\src\\main\\resources\\people.txt";

    public static void main(String[] args) throws AnalysisException {

        SparkSession sparkSession = SparkSession.builder().appName("firstDemo").master("local[*]").getOrCreate();

//        runBasicDataFrameExample(sparkSession);
//        runProgrammaticSchemaExample(sparkSession);
//        runProgrammaticSchemaExampleA(sparkSession);
        createDataFrameByReflect(sparkSession);
    }

    private static void runBasicDataFrameExample(SparkSession sparkSession) throws AnalysisException {
        Dataset<Row> dataset = sparkSession.read().json("E:\\myproject\\hsapi\\hsservice\\src\\main\\resources\\people.json");
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

    private static void runProgrammaticSchemaExample(SparkSession sparkSession){
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(FILE_PATH, 4).toJavaRDD();

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
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(FILE_PATH, 4).toJavaRDD();
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

    /**
     * 反射的方式推导出schema
     * @param sparkSession
     */
    private static void createDataFrameByReflect(SparkSession sparkSession){
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(FILE_PATH, 4).toJavaRDD();
        JavaRDD<People> peopleJavaRDD = javaRDD.map((Function<String, People>) s -> {
            String[] strs = s.split(",");
            return new People(strs[0], Integer.parseInt(strs[1]));
        });

        Dataset<Row> rowJavaRDD = sparkSession.createDataFrame(peopleJavaRDD, People.class);
        rowJavaRDD.show();
        List<Row> list = rowJavaRDD.collectAsList();
        for(Row row : list){
            System.out.println(row.get(0).toString());
            System.out.println(row.get(1).toString());
        }
    }

}
