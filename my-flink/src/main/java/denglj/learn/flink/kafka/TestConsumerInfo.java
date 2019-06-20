package denglj.learn.flink.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.map.HashedMap;

/**
 * Created by denglj on 2019/5/30.
 */
public class TestConsumerInfo {
    public static void main(String[] args) {
        NormalConsumerAdmin consumerAdmin = new NormalConsumerAdmin("cons_500w_ck", "denglj_bdjg", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092", new HashedMap());
        for (TopicPartitionInfo partitionInfo : consumerAdmin.partitionInfos()) {
            System.out.println(JSON.toJSONString(partitionInfo));
        }
    }
}
