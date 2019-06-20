package denglj.learn.flink.vo;

public class Gjxx implements Comparable<Gjxx>{
    private String sfzhm;
    private String xm;
    private String hdfssj;
    private String hddd;

    @Override
    public int compareTo(Gjxx gj) {
        return hdfssj.compareTo(gj.getHdfssj());
    }

    @Override
    public String toString() {
        return xm + ":" + sfzhm;
    }

    public String getHddd() {
        return hddd;
    }

    public void setHddd(String hddd) {
        this.hddd = hddd;
    }

    public String getSfzhm() {
        return sfzhm;
    }

    public void setSfzhm(String sfzhm) {
        this.sfzhm = sfzhm;
    }

    public String getXm() {
        return xm;
    }

    public void setXm(String xm) {
        this.xm = xm;
    }

    public String getHdfssj() {
        return hdfssj;
    }

    public void setHdfssj(String hdfssj) {
        this.hdfssj = hdfssj;
    }
}
