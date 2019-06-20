package denglj.learn.flink.rybd.test;

import denglj.learn.flink.kafka.TopicPartitionInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by denglj on 2019/5/22.
 */
public class TestConsumeData {

    private static Logger log = LoggerFactory.getLogger(TestConsumeData.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092");
        props.put("group.id", "cons-test4");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("denglj-topic-monitor"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }

    public static void partitionInfo(KafkaConsumer<String, String> consumer, String topic){
//            List<TopicPartitionInfo> topicPartitionInfos = new ArrayList<>();
        try {
            log.info("查询分区信息开始...");
            Map<String, Object> properties = new HashMap();
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);

            List<TopicPartition> topicPartitionList = new ArrayList<TopicPartition>();
            for(PartitionInfo info : partitionInfoList){
                topicPartitionList.add(new TopicPartition(info.topic(),info.partition()));
            }

            //分配分区 为获取当前消费值
//                consumer.assign(topicPartitionList);

            for (TopicPartition topicPartition : topicPartitionList) {
                long offsets = consumer.position(topicPartition);
                long beginOffsets = consumer.beginningOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                long endOffsets = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                log.info("topic:"+topicPartition.topic()+"\t分区:"+topicPartition.partition()+"\t消费偏移量:"+offsets+"\tbegin偏移量:"+beginOffsets+"\tend偏移量:"+endOffsets);
//                    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo();
//                    topicPartitionInfo.setTopic(topicPartition.topic());
//                    topicPartitionInfo.setOffsets(offsets);
//                    topicPartitionInfo.setPartition(topicPartition.partition());
//                    topicPartitionInfo.setBeginOffsets(beginOffsets);
//                    topicPartitionInfo.setEndOffsets(endOffsets);
//                    topicPartitionInfos.add(topicPartitionInfo);
            }
            log.info("查询分区信息结束...");
        }catch (Exception e){
            log.error("获取分区信息失败:{}",e.getMessage());
            e.printStackTrace();
        }
    }
}
