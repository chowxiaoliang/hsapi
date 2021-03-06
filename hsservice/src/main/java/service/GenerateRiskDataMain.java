package service;

import bean.RiskEvent;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import hbase.CoreOperationService;
import hbase.CoreOperationServiceImpl;
import service.risk.DataUtils;
import service.risk.GenerateName;
import service.risk.TimeUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class GenerateRiskDataMain {

    /**十亿条记录*/
    private final static int TIME = 10000000;

    private final static String COUNTRY = "中国";

    private final static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws IOException {
        CoreOperationService coreOperationService = new CoreOperationServiceImpl();

        for(int i = 0;i < TIME; i++){
            RiskEvent riskEvent = new RiskEvent();
            riskEvent.setPartnerId(DataUtils.getPartnerId());
            riskEvent.setRiskFlowNo(DataUtils.getRiskFlowNo());
            riskEvent.setEventType(DataUtils.getEventType());
            riskEvent.setCertNo(DataUtils.getCertNo());
            riskEvent.setName(GenerateName.getName());
            riskEvent.setMobile(DataUtils.getMobile());
            riskEvent.setBankCard(DataUtils.getBankCardNo());
            riskEvent.setCountry(COUNTRY);
            riskEvent.setProvince(DataUtils.getProvince());
            riskEvent.setCity(DataUtils.getCity(riskEvent.getProvince()));
            riskEvent.setOccurTimeLong(TimeUtils.getTimeLong());
            riskEvent.setOccurTimeStr(SIMPLE_DATE_FORMAT.format(riskEvent.getOccurTimeLong()));
            riskEvent.setOccurTimeDateToStr(new Date(riskEvent.getOccurTimeLong()).toString());
            riskEvent.setMac(DataUtils.getMac());
            riskEvent.setImei(DataUtils.getImei());
            riskEvent.setPhoneOperator(DataUtils.getOperator());
            riskEvent.setPhoneMarker(DataUtils.getPhoneMarker());
            riskEvent.setOs(DataUtils.getOs());
            riskEvent.setDeliverAddressCountry(COUNTRY);
            riskEvent.setDeliverAddressProvince(DataUtils.getProvince());
            riskEvent.setDeliverAddressCity(riskEvent.getDeliverAddressProvince());
            riskEvent.setDeliverZipCode(String.valueOf(DataUtils.getRandom(100000, 999999)));
            riskEvent.setDeliverMobileNo(DataUtils.getMobile());
            riskEvent.setDeliverName(GenerateName.getName());
            riskEvent.setBankName(DataUtils.getBankName());
            riskEvent.setBankCardNo(DataUtils.getBankCardNo());
            riskEvent.setOrigOrderId(UUID.randomUUID().toString());
            riskEvent.setPayeeName(GenerateName.getName());
            riskEvent.setPayeeEmail(DataUtils.getEmail());
            riskEvent.setPayeeMobile(DataUtils.getMobile());
            riskEvent.setPayeeCardNumber(DataUtils.getBankCardNo());
            riskEvent.setPayeeIdNumber(DataUtils.getCertNo());
            riskEvent.setPayId(UUID.randomUUID().toString().replace("-", ""));
            riskEvent.setPayMethod(DataUtils.getPayMethod());

            JSONObject jsonObject = (JSONObject) JSON.toJSON(riskEvent);
            String rowKey = jsonObject.getString("riskFlowNo");
            try {
                coreOperationService.addBatchRowData("RISK_EVENT", rowKey, jsonObject);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
