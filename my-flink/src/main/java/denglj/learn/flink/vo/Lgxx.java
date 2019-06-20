package denglj.learn.flink.vo;

import java.io.Serializable;

/**
 * Created by denglj on 2019/5/5.
 */
public class Lgxx implements Serializable {
    private String rzsj;
    private String sfzhm;
    private String lgbh;

    public String getRzsj() {
        return rzsj;
    }

    public void setRzsj(String rzsj) {
        this.rzsj = rzsj;
    }

    public String getSfzhm() {
        return sfzhm;
    }

    public void setSfzhm(String sfzhm) {
        this.sfzhm = sfzhm;
    }

    public String getLgbh() {
        return lgbh;
    }

    public void setLgbh(String lgbh) {
        this.lgbh = lgbh;
    }
}
