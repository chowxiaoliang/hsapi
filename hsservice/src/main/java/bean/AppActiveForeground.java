package bean;

/**
 * 用户前台活跃
 */
public class AppActiveForeground {

    private String pushId;//推送的消息的id，如果不是从推送消息打开，传空
    private String access;//1.push 2.icon 3.其他

    public String getPushId() {
        return pushId;
    }

    public void setPushId(String pushId) {
        this.pushId = pushId;
    }

    public String getAccess() {
        return access;
    }

    public void setAccess(String access) {
        this.access = access;
    }
}
