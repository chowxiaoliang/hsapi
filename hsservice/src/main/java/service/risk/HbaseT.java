package service.risk;

import hbase.CoreOperationService;
import hbase.CoreOperationServiceImpl;

import java.io.IOException;

/**
 * Created by Administrator on 2020/7/2 0002.
 */
public class HbaseT {

    public static void main(String[] args) throws IOException {

        CoreOperationService coreOperationService = new CoreOperationServiceImpl();
        boolean result = coreOperationService.isTableExist("RISK_EVENT");
        System.out.println(result);

        System.out.println(DataUtils.getEventType());
        System.out.println(DataUtils.getOperator());

    }
}
