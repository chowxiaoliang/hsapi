package hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @desc 一些核心操作api的具体实现
 */
public class CoreOperationServiceImpl implements CoreOperationService {

    private static Connection CONNECTION = null;

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop001");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            CONNECTION = ConnectionFactory.createConnection(configuration);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public boolean isTableExist(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = (HBaseAdmin)CONNECTION.getAdmin();
        return hBaseAdmin.tableExists(tableName);
    }

    @Override
    public void createTable(String tableName, String... columnFamily) throws IOException {
        HBaseAdmin hBaseAdmin = (HBaseAdmin)CONNECTION.getAdmin();
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
        HBaseAdmin hBaseAdmin = (HBaseAdmin)CONNECTION.getAdmin();
        if(!isTableExist(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
    }

    @Override
    public void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        Table hTable = CONNECTION.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
    }

    @Override
    public void addBatchRowData(String tableName, String rowKey, Map<String, String> columnMap) throws IOException {
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        // key使用md5加密
        String md5Key = md5HashRowKey(rowKey);
        Put put = new Put(md5Key.getBytes());
        for(String entry : columnMap.keySet()){
            String jsonData = columnMap.get(entry);
            JSONObject jsonObject = JSONObject.parseObject(jsonData);
            for(String innerEntry : jsonObject.keySet()){
                put.addColumn(Bytes.toBytes(entry), Bytes.toBytes(innerEntry), Bytes.toBytes(innerEntry));
            }
        }
        table.put(put);
    }

    @Override
    public void deleteMultiRow(String tableName, String... rows) throws IOException {
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    @Override
    public void getAllRows(String tableName) throws IOException {
        Table hTable = CONNECTION.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    @Override
    public void getRow(String tableName, String rowKey) throws IOException {
        Table hTable = CONNECTION.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        // get.setMaxVersions();显示所有版本
        // get.setTimeStamp();显示指定时间戳的版本
        Result result = hTable.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    @Override
    public void getRowQualifier(String tableName, String rowKey, String columnFamily, String qualifier) throws IOException {
        Table hTable = CONNECTION.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    /**
     * 取key值的MD5前8位再拼接key值
     */
    private String md5HashRowKey(String key){
        return MD5Hash.getMD5AsHex(key.getBytes()).substring(0, 8) + key;
    }

}
