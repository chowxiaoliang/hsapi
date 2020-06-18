package bean;

import java.io.Serializable;

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
    /**收货人座机号*/
    private String deliverPhone;
    /**收货人姓名*/
    private String deliverName;
    /**银行卡类型*/
    private String bankCardType;
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
    /**卖家或收款人座机*/
    private String payeePhone;
    /**卖家或收款人身份证*/
    private String payeeIdNumber;
    /**卖家或收款人银行卡号*/
    private String payeeCardNumber;
    /**内部支付流水号*/
    private String payId;
    /**支付方式*/
    private String payMethod;

}
