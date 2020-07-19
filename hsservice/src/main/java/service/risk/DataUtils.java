package service.risk;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataUtils {

    private final static Random RANDOM_PARTNER = new Random();

    private final static Random RANDOM_PROVINCE = new Random();

    private final static Random RANDOM_CITY = new Random();

    private final static Random RANDOM_MAC = new Random();

    private final static Random RANDOM_OPERATOR = new Random();

    private final static Random RANDOM_PHONE_MARKER = new Random();

    private final static Random RANDOM_OS = new Random();

    private final static Random RANDOM_BANK = new Random();

    private final static Random RANDOM_PAY_METHOD = new Random();

    private final static Random RANDOM_EVENT_TYPE = new Random();

    private final static LocationUtil LOCATION_UTIL = LocationUtil.getInstance();

    private final static SnowFlake SNOW_FLAKE_RISKFLOW_NO = new SnowFlake(1, 1);

    public static String getPartnerId(){
        List<String> partnerList = new ArrayList<>();
        partnerList.add("xwbank");
        partnerList.add("zyxj");
        partnerList.add("xiaoniu");
        partnerList.add("qianbaba");
        partnerList.add("qufenqi");
        partnerList.add("ztx");
        partnerList.add("ssj");
        partnerList.add("huaxiabank");
        partnerList.add("zhaoshangbank");
        partnerList.add("shanghaibank");
        partnerList.add("wangshangbank");
        partnerList.add("weizongbank");
        partnerList.add("weLab");
        partnerList.add("qidai");
        partnerList.add("madai");
        partnerList.add("lexin");
        partnerList.add("miaoche");
        partnerList.add("zhaolian");
        partnerList.add("guangfabank");
        partnerList.add("yizhangtong");


        int position = RANDOM_PARTNER.nextInt(20);

        return partnerList.get(position);
    }

    public static String getRiskFlowNo(){
        // 一位大写字母+雪花算法生成的id
        char c = (char) getRandom(65, 90);
        return String.valueOf(c) + SNOW_FLAKE_RISKFLOW_NO.nextId();
    }

    public static String getCertNo(){
        String[] coefficientArray = { "7","9","10","5","8","4","2","1","6","3","7","9","10","5","8","4","2","1"};
        String[] lastNumberArray = { "1","0","X","9","8","7","6","5","4","3","2"};
        // 18位身份证号码各位的含义:
        // 1-2位省、自治区、直辖市代码；
        // 3-4位地级市、盟、自治州代码；
        // 5-6位县、县级市、区代码；
        // 7-14位出生年月日，比如19670401代表1967年4月1日；
        // 15-17位为顺序号，其中17位（倒数第二位）男为单数，女为双数；
        // 18位为校验码，0-9和X。
        // 作为尾号的校验码，是由把前十七位数字带入统一的公式计算出来的，
        // 计算的结果是0-10，如果某人的尾号是0－9，都不会出现X，但如果尾号是10，那么就得用X来代替，
        // 因为如果用10做尾号，那么此人的身份证就变成了19位。X是罗马数字的10，用X来代替10
        String id = "";
        // 随机生成省、自治区、直辖市代码 1-2
        String[] provinces = { "11", "12", "13", "14", "15", "21", "22", "23",
                "31", "32", "33", "34", "35", "36", "37", "41", "42", "43",
                "44", "45", "46", "50", "51", "52", "53", "54", "61", "62",
                "63", "64", "65", "71", "81", "82" };
        String province = provinces[new Random().nextInt(provinces.length - 1)];
        // 随机生成地级市、盟、自治州代码 3-4
        String[] citys = { "01", "02", "03", "04", "05", "06", "07", "08",
                "09", "10", "21", "22", "23", "24", "25", "26", "27", "28" };
        String city = citys[new Random().nextInt(citys.length - 1)];
        // 随机生成县、县级市、区代码 5-6
        String[] countys = { "01", "02", "03", "04", "05", "06", "07", "08",
                "09", "10", "21", "22", "23", "24", "25", "26", "27", "28",
                "29", "30", "31", "32", "33", "34", "35", "36", "37", "38" };
        String county = countys[new Random().nextInt(countys.length - 1)];
        // 随机生成出生年月 7-14
        SimpleDateFormat dft = new SimpleDateFormat("yyyyMMdd");
        Date beginDate = new Date();
        Calendar date = Calendar.getInstance();
        date.setTime(beginDate);
        date.set(Calendar.DATE,
                date.get(Calendar.DATE) - (365 * 30 + new Random().nextInt(365 * 20)));
        String birth = dft.format(date.getTime());
        // 随机生成顺序号 15-17
        String no = String.format("%03d", new Random().nextInt(999));
        // 拼接身份证号码
        id = province + city + county + birth + no;
        int total = 0;
        for(int i=0;i<17;i++) {
            total = total + Integer.parseInt(id.substring(i, i+1))*Integer.parseInt(coefficientArray[i]);
        }
        // 生成身份证最后一位数
        String check = lastNumberArray[total%11];
        id = id + check;
        return id;
    }

    public static String getMobile(){
        //给予真实的初始号段，号段是在百度上面查找的真实号段
        String[] start = {"133", "149", "153", "173", "177",
                "180", "181", "189", "199", "130", "131", "132",
                "145", "155", "156", "166", "171", "175", "176", "185", "186", "166", "134", "135",
                "136", "137", "138", "139", "147", "150", "151", "152", "157", "158", "159", "172",
                "178", "182", "183", "184", "187", "188", "198", "170", "171"};

        //随机出真实号段   使用数组的length属性，获得数组长度，
        //通过Math.random（）*数组长度获得数组下标，从而随机出前三位的号段
        String phoneFirstNum = start[(int) (Math.random() * start.length)];
        //随机出剩下的8位数
        String phoneLastNum = "";
        //定义尾号，尾号是8位
        final int nextNum = 8;
        //循环剩下的位数
        for (int i = 0; i < nextNum; i++) {
            //每次循环都从0~9挑选一个随机数
            phoneLastNum = phoneLastNum + (int)(Math.random() * 10);
        }
        //最终将号段和尾数连接起来
        return phoneFirstNum + phoneLastNum;
    }

    public static String getBankCardNo(){
        return GenerateBankCardNo.getBankAccount();
    }

    public static String getProvince(){
        List<String> provinceList = LOCATION_UTIL.getProvinces("中国");
        return provinceList.get(RANDOM_PROVINCE.nextInt(provinceList.size()));
    }

    public static String getCity(String province){
        List<String> cityList = LOCATION_UTIL.getCities("中国", province);
        return cityList.get(RANDOM_CITY.nextInt(cityList.size()));
    }

    public static String getEmail(){
        return GenerateEmail.getEmail(9, 9);
    }

    public static String getMac(){
        String separatorOfMac = ":";
        String[] mac = {String.format("%02x", 0x52), String.format("%02x", 0x54), String.format("%02x", 0x00), String.format("%02x", RANDOM_MAC.nextInt(0xff)), String.format("%02x", RANDOM_MAC.nextInt(0xff)), String.format("%02x", RANDOM_MAC.nextInt(0xff))};
        return String.join(separatorOfMac, mac);
    }

    public static String getEventType(){
        String[] eventType = {"register", "login", "loan", "pay", "transfer", "lend"};
        return eventType[RANDOM_EVENT_TYPE.nextInt(eventType.length)];
    }

    public static String getImei(){
        int r1 = 1000000 + new java.util.Random().nextInt(9000000);
        int r2 = 1000000 + new java.util.Random().nextInt(9000000);
        String input = r1 + "" + r2;
        char[] ch = input.toCharArray();
        int a = 0, b = 0;
        for (int i = 0; i < ch.length; i++) {
            int tt = Integer.parseInt(ch[i] + "");
            if (i % 2 == 0) {
                a = a + tt;
            } else {
                int temp = tt * 2;
                b = b + temp / 10 + temp % 10;
            }
        }
        int last = (a + b) % 10;
        if (last != 0) {
            last = 10 - last;
        }
        return input + last;
    }

    public static String getOperator(){
        String[] operator = {"中国移动", "中国联通", "中国电信"};
        return operator[RANDOM_OPERATOR.nextInt(operator.length)];
    }

    public static String getPhoneMarker(){
        String[] phoneMarker = {"苹果", "小米", "华为", "三星", "谷歌", "荣耀"};
        return phoneMarker[RANDOM_PHONE_MARKER.nextInt(phoneMarker.length)];
    }

    public static String getOs(){
        String[] os = {"Android", "iOS"};
        return os[RANDOM_OS.nextInt(os.length)];
    }

    public static String getBankName(){
        String[] bank = {"招商银行", "华夏银行", "农业银行", "兴业银行", "工商银行", "邮政储蓄", "建设银行", "中国银行"};
        return bank[RANDOM_BANK.nextInt(bank.length)];
    }

    public static String getPayMethod(){
        String[] payMethod = {"支付宝", "微信", "银行卡"};
        return payMethod[RANDOM_PAY_METHOD.nextInt(payMethod.length)];
    }

    public static void main(String[] args) {
        for (int i=0;i<100;i++){
            System.out.println(getPhoneMarker());
        }
    }

    /**
     * 得到两个整数之间任意的随机数
     * @param start
     * @param end
     * @return
     */
    public static int getRandom(int start, int end){
        return (int)(Math.random() * (end-start+1) + start);
    }
}
