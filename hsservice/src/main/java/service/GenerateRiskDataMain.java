package service;

import bean.RiskEvent;
import com.alibaba.fastjson.JSONObject;
import com.google.inject.internal.asm.$MethodAdapter;
import service.risk.DataUtils;
import service.risk.GenerateName;

import java.util.UUID;

public class GenerateRiskDataMain {

    /**十万条记录*/
    private final static int TIME = 1000000000;

    private final static String COUNTRY = "中国";

    public static void main(String[] args) {
        for(int i = 0;i < TIME; i++){
            RiskEvent riskEvent = new RiskEvent();
            riskEvent.setPartnerId(DataUtils.getPartnerId());
            riskEvent.setRiskFlowNo(DataUtils.getRiskFlowNo());
            riskEvent.setCertNo(DataUtils.getCertNo());
            riskEvent.setName(GenerateName.getName());
            riskEvent.setMobile(DataUtils.getMobile());
            riskEvent.setBankCard(DataUtils.getBankCardNo());
            riskEvent.setCountry(COUNTRY);
            riskEvent.setProvince(DataUtils.getProvince());
            riskEvent.setCity(DataUtils.getCity(riskEvent.getProvince()));
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
            riskEvent.setPayId(UUID.randomUUID().toString());
            riskEvent.setPayMethod(DataUtils.getPayMethod());
            System.out.println(JSONObject.toJSONString(riskEvent));
        }
    }
}
