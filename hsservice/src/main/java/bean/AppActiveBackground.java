package bean;

/**
 * 用户后台活跃
 */
public class AppActiveBackground {

    private String activeSource;//1=upgrade,2=download(下载),3=plugin_upgrade

    public String getActiveSource() {
        return activeSource;
    }

    public void setActiveSource(String activeSource) {
        this.activeSource = activeSource;
    }
}
