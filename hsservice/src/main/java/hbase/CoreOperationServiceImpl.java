package hbase;

import com.google.inject.internal.cglib.core.$Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @desc 一些核心操作api的具体实现
 */
public class CoreOperationServiceImpl implements CoreOperationService {

    private static Configuration configuration;
    static {
        HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop001");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
    }

    @Override
    public boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        HBaseAdmin hBaseAdmin = (HBaseAdmin)connection.getAdmin();
        return hBaseAdmin.tableExists(tableName);
    }

    @Override
    public void createTable(String tableName, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        HBaseAdmin hBaseAdmin = (HBaseAdmin)connection.getAdmin();
        if(isTableExist(tableName)){
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String column : columnFamily){
            hTableDescriptor.addFamily(new HColumnDescriptor(column));
        }
        hBaseAdmin.createTable(hTableDescriptor);
    }

    @Override
    public void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        HBaseAdmin hBaseAdmin = (HBaseAdmin)connection.getAdmin();
        if(!isTableExist(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
    }

    @Override
    public void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
    }

    @Override
    public void deleteMultiRow(String tableName, String... rows) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    @Override
    public void getAllRows(String tableName) {

    }

    @Override
    public void getRow(String tableName, String rowKey) {

    }

    @Override
    public void getRowQualifier(String tableName, String rowKey, String columnFamily, String qualifier) {

    }
}
