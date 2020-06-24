package bean;

import java.io.Serializable;
import java.util.Date;

/**
 * riskEvent bean
 */
public class RiskEvent implements Serializable {

    private static final long serialVersionUID = -7928292629442495988L;

    // baseInfo

    /**合作方编号*/
    private String partnerId;
    /**风控流水号*/
    private String riskFlowNo;
    /**身份证号*/
    private String certNo;
    /**姓名*/
    private String name;
    /**手机号*/
    private String mobile;
    /**银行卡号*/
    private String bankCard;
    /**国家*/
    private String country;
    /**省份*/
    private String province;
    /**城市*/
    private String city;
    /**事件发生时间*/
    private long occurTimeLong;
    private String occurTimeStr;
    private String occurTimeDateToStr;

    // deviceInfo

    /**MAC地址*/
    private String mac;
    /**IMEI*/
    private String imei;
    /**运营商*/
    private String phoneOperator;
    /**手机制造商*/
    private String phoneMarker;
    /**操作系统类型*/
    private String os;

    // deliveryInfo

    /**收货国家*/
    private String deliverAddressCountry;
    /**收货省份*/
    private String deliverAddressProvince;
    /**收货城市*/
    private String deliverAddressCity;
    /**收货邮编信息*/
    private String deliverZipCode;
    /**收货人手机号*/
    private String deliverMobileNo;
    /**收货人姓名*/
    private String deliverName;
    /**银行名称*/
    private String bankName;
    /**银行卡号*/
    private String bankCardNo;
    /**原系统订单号*/
    private String origOrderId;


    // sellerInfo

    /**卖家或收款人姓名*/
    private String payeeName;
    /**卖家或收款人邮箱*/
    private String payeeEmail;
    /**卖家或收款人手机*/
    private String payeeMobile;
    /**卖家或收款人身份证*/
    private String payeeIdNumber;
    /**卖家或收款人银行卡号*/
    private String payeeCardNumber;
    /**内部支付流水号*/
    private String payId;
    /**支付方式*/
    private String payMethod;

    public String getPartnerId() {
        return partnerId;
    }

    public void setPartnerId(String partnerId) {
        this.partnerId = partnerId;
    }

    public String getRiskFlowNo() {
        return riskFlowNo;
    }

    public void setRiskFlowNo(String riskFlowNo) {
        this.riskFlowNo = riskFlowNo;
    }

    public String getCertNo() {
        return certNo;
    }

    public void setCertNo(String certNo) {
        this.certNo = certNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getBankCard() {
        return bankCard;
    }

    public long getOccurTimeLong() {
        return occurTimeLong;
    }

    public void setOccurTimeLong(long occurTimeLong) {
        this.occurTimeLong = occurTimeLong;
    }

    public String getOccurTimeStr() {
        return occurTimeStr;
    }

    public void setOccurTimeStr(String occurTimeStr) {
        this.occurTimeStr = occurTimeStr;
    }

    public String getOccurTimeDateToStr() {
        return occurTimeDateToStr;
    }

    public void setOccurTimeDateToStr(String occurTimeDateToStr) {
        this.occurTimeDateToStr = occurTimeDateToStr;
    }

    public void setBankCard(String bankCard) {
        this.bankCard = bankCard;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getPhoneOperator() {
        return phoneOperator;
    }

    public void setPhoneOperator(String phoneOperator) {
        this.phoneOperator = phoneOperator;
    }

    public String getPhoneMarker() {
        return phoneMarker;
    }

    public void setPhoneMarker(String phoneMarker) {
        this.phoneMarker = phoneMarker;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getDeliverAddressCountry() {
        return deliverAddressCountry;
    }

    public void setDeliverAddressCountry(String deliverAddressCountry) {
        this.deliverAddressCountry = deliverAddressCountry;
    }

    public String getDeliverAddressProvince() {
        return deliverAddressProvince;
    }

    public void setDeliverAddressProvince(String deliverAddressProvince) {
        this.deliverAddressProvince = deliverAddressProvince;
    }

    public String getDeliverAddressCity() {
        return deliverAddressCity;
    }

    public void setDeliverAddressCity(String deliverAddressCity) {
        this.deliverAddressCity = deliverAddressCity;
    }

    public String getDeliverZipCode() {
        return deliverZipCode;
    }

    public void setDeliverZipCode(String deliverZipCode) {
        this.deliverZipCode = deliverZipCode;
    }

    public String getDeliverMobileNo() {
        return deliverMobileNo;
    }

    public void setDeliverMobileNo(String deliverMobileNo) {
        this.deliverMobileNo = deliverMobileNo;
    }

    public String getDeliverName() {
        return deliverName;
    }

    public void setDeliverName(String deliverName) {
        this.deliverName = deliverName;
    }

    public String getBankName() {
        return bankName;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public String getBankCardNo() {
        return bankCardNo;
    }

    public void setBankCardNo(String bankCardNo) {
        this.bankCardNo = bankCardNo;
    }

    public String getOrigOrderId() {
        return origOrderId;
    }

    public void setOrigOrderId(String origOrderId) {
        this.origOrderId = origOrderId;
    }

    public String getPayeeName() {
        return payeeName;
    }

    public void setPayeeName(String payeeName) {
        this.payeeName = payeeName;
    }

    public String getPayeeEmail() {
        return payeeEmail;
    }

    public void setPayeeEmail(String payeeEmail) {
        this.payeeEmail = payeeEmail;
    }

    public String getPayeeMobile() {
        return payeeMobile;
    }

    public void setPayeeMobile(String payeeMobile) {
        this.payeeMobile = payeeMobile;
    }

    public String getPayeeIdNumber() {
        return payeeIdNumber;
    }

    public void setPayeeIdNumber(String payeeIdNumber) {
        this.payeeIdNumber = payeeIdNumber;
    }

    public String getPayeeCardNumber() {
        return payeeCardNumber;
    }

    public void setPayeeCardNumber(String payeeCardNumber) {
        this.payeeCardNumber = payeeCardNumber;
    }

    public String getPayId() {
        return payId;
    }

    public void setPayId(String payId) {
        this.payId = payId;
    }

    public String getPayMethod() {
        return payMethod;
    }

    public void setPayMethod(String payMethod) {
        this.payMethod = payMethod;
    }
}
