package denglj.learn.flink.kafka.consumer;

/**
 * Created by denglj on 2019/5/30.
 */
public class MyPartitionMetric implements Comparable<MyPartitionMetric>{
    private String topic;
    private int partition;
    private long current;
    private long end;
    private long lag;
    private long process;
    private long time;

    @Override
    public int compareTo(MyPartitionMetric o) {
        if(time - o.getTime() > 0){
            return 1;
        }else{
            return -1;
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getProcess() {
        return process;
    }

    public void setProcess(long process) {
        this.process = process;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getCurrent() {
        return current;
    }

    public void setCurrent(long current) {
        this.current = current;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }
}
