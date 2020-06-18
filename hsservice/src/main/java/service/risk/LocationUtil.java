package service.risk;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LocationUtil {
    /**
     * 各地区xml文件路径
     */
    private static final String LOCAL_LIST_PATH = "D:\\projectCode\\hsapi\\hsservice\\src\\main\\resources\\LocationList.xml";
    /**
     * 所有国家名称List
     */
    private static final List<String> COUNTRY_REGION = new ArrayList<>();
    private static final List<String> PROVINCE = new ArrayList<>();
    private static LocationUtil localutil;
    private SAXReader reader;
    private Document document;
    private Element rootElement;

    /**初始化*/
    private LocationUtil(){
        reader = new SAXReader();
        try {
            document = reader.read(LOCAL_LIST_PATH);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        //2.获得根元素
        rootElement =  document.getRootElement();
        //3.初始化所有国家名称列表
        Iterator it =  rootElement.elementIterator();
        Element ele = null;
        while(it.hasNext()){
            ele = (Element)it.next();
            COUNTRY_REGION.add(ele.attributeValue("Name"));
        }

        // 添加中国所有的省份到集合

    }

    public List<String> getCountry(){
        return COUNTRY_REGION;
    }

    private List<Element> provinces(String countryName){
        Iterator it =  rootElement.elementIterator();
        List<Element> provinces = new ArrayList<Element>();
        Element ele = null;
        while(it.hasNext()){
            ele = (Element)it.next();
            if(ele.attributeValue("Name").equals(countryName)){
                provinces = ele.elements();
                break;
            }
        }
        return provinces;
    }

    public List<String> getProvinces(String countryName){
        List<Element> tmp = this.provinces(countryName);
        List<String> list = new ArrayList<String>();
        for(int i=0; i<tmp.size(); i++){
            list.add(tmp.get(i).attributeValue("Name"));
        }
        return list;
    }

    private List<Element> cities(String countryName, String provinceName){
        List<Element> provinces =  this.provinces(countryName);
        List<Element> cities = new ArrayList<Element>();
        if(provinces==null || provinces.size()==0){
            return cities;
        }

        for(int i=0; i<provinces.size(); i++){
            if(provinces.get(i).attributeValue("Name").equals(provinceName)){
                cities = provinces.get(i).elements();
                break;
            }
        }
        return cities;
    }

    public List<String> getCities(String countryName, String provinceName){
        List<Element> tmp =  this.cities(countryName, provinceName);
        List<String> cities = new ArrayList<String>();
        for(int i=0; i<tmp.size(); i++){
            cities.add(tmp.get(i).attributeValue("Name"));
        }
        return cities;
    }

    public static LocationUtil getInstance(){
        if(localutil==null){
            localutil = new LocationUtil();
        }
        return localutil;
    }

    public static void main(String[] args) {
        LocationUtil lu =  LocationUtil.getInstance();
        List<String> list = lu.getCities("中国", "广东");
        for(int i=0; i<list.size(); i++){
            System.out.print(list.get(i) + " ");
        }
    }
}
