package hbase;

/**
 * @desc hbase 一些核心操作api
 */
public interface CoreOperationService {

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    boolean isTableExist(String tableName);

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     */
    void createTable(String tableName, String... columnFamily);

    /**
     * 删除表
     * @param tableName
     */
    void dropTable(String tableName);

    /**
     * 向表中插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    void addRowData(String tableName, String rowKey, String columnFamily, String column, String value);

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     */
    void deleteMultiRow(String tableName, String... rows);

    /**
     * 获取所有数据
     * @param tableName
     */
    void getAllRows(String tableName);

    /**
     * 根据行键获取一行数据
     * @param tableName
     * @param rowKey
     */
    void getRow(String tableName, String rowKey);

    /**
     * 根据某一行指定的（列族：列 ）获取数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     */
    void getRowQualifier(String tableName, String rowKey, String columnFamily, String qualifier);
}
