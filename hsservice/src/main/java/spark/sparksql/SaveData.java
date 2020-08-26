package spark.sparksql;

import org.apache.spark.streaming.api.java.JavaDStream;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static spark.ConnectionPool.getConnection;
import static spark.ConnectionPool.release;

public class SaveData {

    /**
     * 将流数据处理后存到mysql
     * @param javaDStream
     */
    public void saveDataToMysql(JavaDStream<String> javaDStream){
        javaDStream.foreachRDD(stringJavaRDD -> {
            stringJavaRDD.foreachPartition(x -> {
                Connection connection = getConnection();
                String sql = "";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                while (x.hasNext()){
                    preparedStatement.setString(1, x.toString());
                    // 批量执行sql相关的操作
                    // 当分区中的数据量很大时，preparedStatement.addBatch()缓存能力可能不够，所以在添加到缓存中时，可以让它
                    // 5万执行一次，preparedStatement.executeBatch，可以添加一个变量计数就可以了。
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                preparedStatement.close();
                release(connection);
            });
        });
    }
}
