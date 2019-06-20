package denglj.learn.flink.kafka.consumer;

/**
 * Created by denglj on 2019/5/30.
 */
public class MyConsumerStat {
    private int total;
    private long lag;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }
}
