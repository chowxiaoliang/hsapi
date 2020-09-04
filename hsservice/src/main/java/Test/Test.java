package Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 方法参数里面传递对象里面的引用，然后计算
 */
public class Test {

    public static void main(String[] args) {
        People people = new People("420984", "zhouliang", "15872151893");
        Test test = new Test();
        test.calcPeople(people.getCertNo(), people.getName(), people.getPhone(), people.getCompany(), people.getSchoolList());
        System.out.println("个人信息处理完成！");
        System.out.println(people.toString());
    }

    private void calcPeople(String certNo, String name, String phone, Company company, List<String> schoolList){
        System.out.println("开始处理个人信息！");
        System.out.println("certNo="+certNo+",name="+name+",phone="+phone);
        company.setDemptNo("123");
        company.setEmptName("Test");
        company.setEmptNo("456");
        schoolList.add("长大");
        schoolList.add("北理");
    }

    static class People{
        private String certNo;
        private String name;
        private String phone;
        private Company company = new Company();
        private List<String> schoolList = new ArrayList<>();

        public People(String certNo, String name, String phone){
            this.certNo = certNo;
            this.name = name;
            this.phone = phone;
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

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        public Company getCompany() {
            return company;
        }

        public void setCompany(Company company) {
            this.company = company;
        }

        public List<String> getSchoolList() {
            return schoolList;
        }

        public void setSchoolList(List<String> schoolList) {
            this.schoolList = schoolList;
        }

        @Override
        public String toString() {
            return "People{" +
                    "certNo='" + certNo + '\'' +
                    ", name='" + name + '\'' +
                    ", phone='" + phone + '\'' +
                    ", company=" + company +
                    ", schoolList=" + schoolList +
                    '}';
        }
    }

    static class Company{
        private String emptName;
        private String emptNo;
        private String demptNo;

        public String getEmptName() {
            return emptName;
        }

        public void setEmptName(String emptName) {
            this.emptName = emptName;
        }

        public String getEmptNo() {
            return emptNo;
        }

        public void setEmptNo(String emptNo) {
            this.emptNo = emptNo;
        }

        public String getDemptNo() {
            return demptNo;
        }

        public void setDemptNo(String demptNo) {
            this.demptNo = demptNo;
        }

        @Override
        public String toString() {
            return "Company{" +
                    "emptName='" + emptName + '\'' +
                    ", emptNo='" + emptNo + '\'' +
                    ", demptNo='" + demptNo + '\'' +
                    '}';
        }
    }
}
