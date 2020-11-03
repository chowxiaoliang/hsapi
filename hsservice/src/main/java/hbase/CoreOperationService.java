package hbase;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @desc hbase 一些核心操作api
 */
public interface CoreOperationService {

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    boolean isTableExist(String tableName) throws IOException;

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     */
    void createTable(String tableName, String... columnFamily) throws IOException;

    /**
     * 删除表
     * @param tableName
     */
    void dropTable(String tableName) throws IOException;

    /**
     * 向表中插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException;

    /**
     * 向表中批量插入数据,多个columnFamily的情况，数据为json字符串
     * @param tableName
     * @param rowKey
     * @param jsonData json数据，直接按对应的key获取数据
     */
    void addBatchRowData(String tableName, String rowKey, JSONObject jsonData) throws IOException;

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     */
    void deleteMultiRow(String tableName, String... rows) throws IOException;

    /**
     * 获取所有数据
     * @param tableName
     */
    void getAllRows(String tableName) throws IOException;

    /**
     * 根据行键获取一行数据
     * @param tableName
     * @param rowKey
     */
    void getRow(String tableName, String rowKey) throws IOException;

    /**
     * 根据行键集合获取多行数据
     * @param tableName
     * @param rowKeyList
     * @throws IOException
     */
    void getMultiRows(String tableName, List<String> rowKeyList) throws IOException;

    /**
     * 根据某一行指定的（列族：列 ）获取数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     */
    void getRowQualifier(String tableName, String rowKey, String columnFamily, String qualifier) throws IOException;

    /**
     * 根据scan和filter获取多行数据
     * @param tableName
     * @throws IOException
     */
    void getRowsByScanAndFilter(String tableName) throws IOException;
}
