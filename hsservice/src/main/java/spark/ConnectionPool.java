package spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.Properties;

/**
 * 以连接池的方式管理jdbc数据库连接
 */
public class ConnectionPool {

    private static final String CONFIG_PATH = "/resources/config.properties";
    private static LinkedList<Connection> pool = new LinkedList<>();

    private ConnectionPool(){}

    static {
        Properties properties = new Properties();
        try {
            properties.load(ConnectionPool.class.getClassLoader().getResourceAsStream("config.properties"));
            Class.forName(properties.getProperty(Constant.JDBC_DRIVER));
            String url = properties.getProperty(Constant.JDBC_URL);
            String user = properties.getProperty(Constant.JDBC_USER);
            String password = properties.getProperty(Constant.JDBC_PASSWORD);
            int maxActive = Integer.parseInt(properties.getProperty(Constant.JDBC_MAX_ACTIVE));
            for (int i = 0;i < maxActive; i ++){
                Connection connection = DriverManager.getConnection(url, user, password);
                pool.push(connection);
            }
        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        while(pool.isEmpty()) {
            try {
                System.out.println("线程池为空，请稍后再来~~~~~");
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return pool.poll();
    }

    public static void release(Connection connection){
        pool.push(connection);
    }

    private interface Constant{
        String JDBC_DRIVER = "jdbc.driver";
        String JDBC_URL = "jdbc.url";
        String JDBC_USER = "jdbc.user";
        String JDBC_PASSWORD = "jdbc.password";
        String JDBC_MAX_ACTIVE = "jdbc.max.active";
    }

    public static void main(String[] args) throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String sql = "select * from engine_rule";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            System.out.println(resultSet.getLong("id")+"  "+resultSet.getString("rule_name"));
        }
    }
}
