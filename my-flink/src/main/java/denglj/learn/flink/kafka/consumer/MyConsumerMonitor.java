package denglj.learn.flink.kafka.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by denglj on 2019/5/30.
 */
public class MyConsumerMonitor {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private long lastTime = System.currentTimeMillis();

    private Map<String, MyPartitionMetric> lastMetrics = new HashedMap();

    private KafkaProducer<String, String> producer;

    public MyConsumerMonitor(){
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "192.168.120.120:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props);
    }

    public void gatherInfo(KafkaConsumer<byte[], byte[]> consumer, long processCount){
        if(System.currentTimeMillis() - lastTime > 5 * 1000){
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition topicPartition : assignment) {
                String key = topicPartition.topic() + "-" + topicPartition.partition();
                MyPartitionMetric lastMetric = lastMetrics.get(key);
                long offsets = consumer.position(topicPartition);
                long beginOffsets = consumer.beginningOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                long endOffsets = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                MyPartitionMetric metric = new MyPartitionMetric();
                metric.setCurrent(offsets);
                metric.setPartition(topicPartition.partition());
                metric.setEnd(endOffsets);
                metric.setLag(endOffsets - offsets);
                metric.setProcess(processCount);
                metric.setTime(System.currentTimeMillis());
                metric.setTopic(topicPartition.topic());
                if(lastMetric != null){
                    metric.setProcess(metric.getCurrent() - lastMetric.getCurrent());
                }
                producer.send(new ProducerRecord<String, String>("denglj-topic-monitor", key, JSON.toJSONString(metric)));
                lastMetrics.put(key, metric);
                log.info("topic:"+topicPartition.topic()+"\t分区:"+topicPartition.partition()+"\t消费偏移量:"+offsets+"\tbegin偏移量:"+beginOffsets+"\tend偏移量:"+endOffsets + "\t消费量:"+ metric.getProcess());
            }
            lastTime = System.currentTimeMillis();
        }

    }

}
