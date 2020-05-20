package core;

import scala.util.parsing.json.JSONObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class CompareT {

    public static void main(String[] args) {
        People people1 = new People();
        People people2 = new People();
        People people3 = new People();
        people1.setAge(18).setCertNo("123456").setName("a").setMobile("17896542312");
        people2.setAge(19).setCertNo("654321").setName("b").setMobile("98756987456");
        people3.setAge(20).setCertNo("236598").setName("c").setMobile("69852365478");
        List<People> list = new ArrayList<>();
        list.add(people1);
        list.add(people2);
        list.add(people3);
        for(People people : list){
            System.out.println(people.getAge());
        }
        list.sort(new MyComparator());
        for (People people : list){
            System.out.println(people.getAge());
        }
        List<People> list1 = list.stream().sorted(Comparator.comparingInt(People::getAge)).collect(Collectors.toList());
        list1.sort((p1,p2)-> p2.getAge()-p1.getAge());
    }

    static class MyComparator implements Comparator<People>{

        @Override
        public int compare(People o1, People o2) {
            return o2.getAge()-o1.getAge();
        }
    }


    static class People{
        private String certNo;
        private String name;
        private String mobile;
        private int age;

        public String getCertNo() {
            return certNo;
        }

        public People setCertNo(String certNo) {
            this.certNo = certNo;
            return this;
        }

        public String getName() {
            return name;
        }

        public People setName(String name) {
            this.name = name;
            return this;
        }

        public String getMobile() {
            return mobile;
        }

        public People setMobile(String mobile) {
            this.mobile = mobile;
            return this;
        }

        public int getAge() {
            return age;
        }

        public People setAge(int age) {
            this.age = age;
            return this;
        }
    }
}
