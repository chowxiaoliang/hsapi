package service;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeT {

    private final static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        MyTime myTime = new MyTime();
        myTime.setTimeLong(System.currentTimeMillis());
        myTime.setTimeStr(SIMPLE_DATE_FORMAT.format(System.currentTimeMillis()));
        myTime.setTimeDate(new Date());
        myTime.setTimeDateToStr(myTime.getTimeDate().toString());

        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(myTime);
        System.out.println("timeLong="+jsonObject.getLongValue("timeLong"));
        System.out.println("timeStr="+jsonObject.getString("timeStr"));
        System.out.println("timeDate="+jsonObject.getDate("timeDate"));
        System.out.println("timeDateToStr="+jsonObject.getString("timeDateToStr"));

    }

    static class MyTime{
        private long timeLong;
        private String timeStr;
        private Date timeDate;
        private String timeDateToStr;

        public long getTimeLong() {
            return timeLong;
        }

        public void setTimeLong(long timeLong) {
            this.timeLong = timeLong;
        }

        public String getTimeStr() {
            return timeStr;
        }

        public void setTimeStr(String timeStr) {
            this.timeStr = timeStr;
        }

        public Date getTimeDate() {
            return timeDate;
        }

        public void setTimeDate(Date timeDate) {
            this.timeDate = timeDate;
        }

        public String getTimeDateToStr() {
            return timeDateToStr;
        }

        public void setTimeDateToStr(String timeDateToStr) {
            this.timeDateToStr = timeDateToStr;
        }
    }
}
