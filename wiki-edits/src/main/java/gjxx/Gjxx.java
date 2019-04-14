package gjxx;

/**
 * Created by denglj on 2019/4/14.
 */
public class Gjxx {
    private String hdfssj;
    private String xm;
    private String sfzhm;

    @Override
    public String toString() {
        return xm +":"+ sfzhm + ":" + hdfssj;
    }

    public String getHdfssj() {
        return hdfssj;
    }

    public void setHdfssj(String hdfssj) {
        this.hdfssj = hdfssj;
    }

    public String getXm() {
        return xm;
    }

    public void setXm(String xm) {
        this.xm = xm;
    }

    public String getSfzhm() {
        return sfzhm;
    }

    public void setSfzhm(String sfzhm) {
        this.sfzhm = sfzhm;
    }
}
