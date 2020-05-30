package hbase;

/**
 * @desc 一些核心操作api的具体实现
 */
public class CoreOperationServiceImpl implements CoreOperationService {
    @Override
    public boolean isTableExist(String tableName) {
        return false;
    }

    @Override
    public void createTable(String tableName, String... columnFamily) {

    }

    @Override
    public void dropTable(String tableName) {

    }

    @Override
    public void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) {

    }

    @Override
    public void deleteMultiRow(String tableName, String... rows) {

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
