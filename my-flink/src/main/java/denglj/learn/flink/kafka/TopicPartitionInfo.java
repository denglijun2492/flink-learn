package denglj.learn.flink.kafka;

/**
 * Created by Administrator on 2019/4/9.
 */
public class TopicPartitionInfo {

    private String topic;
    private int partition;
    private long offsets;
    private long beginOffsets;
    private long endOffsets;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffsets() {
        return offsets;
    }

    public void setOffsets(long offsets) {
        this.offsets = offsets;
    }

    public long getBeginOffsets() {
        return beginOffsets;
    }

    public void setBeginOffsets(long beginOffsets) {
        this.beginOffsets = beginOffsets;
    }

    public long getEndOffsets() {
        return endOffsets;
    }

    public void setEndOffsets(long endOffsets) {
        this.endOffsets = endOffsets;
    }

}
