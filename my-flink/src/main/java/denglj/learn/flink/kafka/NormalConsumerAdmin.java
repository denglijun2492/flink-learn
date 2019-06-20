package denglj.learn.flink.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NormalConsumerAdmin{

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private static String STRATEGY_TO_BEGIN="1";
    private static String STRATEGY_TO_END="2";
    private static String STRATEGY_TO_CUSTOM="3";

    private KafkaConsumer<String, String> consumer;

    private String topic;

    private String groupId;

    public NormalConsumerAdmin(String groupId, String topic, String servers, Map<String, Object> properties){
        this.groupId = groupId;
        this.topic = topic;
        properties.put("bootstrap.servers", servers);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(properties);
    }


    public List<TopicPartitionInfo> partitionInfos() {
        List<TopicPartitionInfo> topicPartitionInfos = new ArrayList<>();
        try {
            log.info("查询分区信息开始...");
            Map<String, Object> properties = new HashMap();
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            Collection<TopicPartition> TopicPartitionList = transforTopicPartition(partitionInfoList);
            //分配分区 为获取当前消费值
            consumer.assign(TopicPartitionList);

            for (TopicPartition topicPartition : TopicPartitionList) {
                long offsets = consumer.position(topicPartition);
                long beginOffsets = consumer.beginningOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                long endOffsets = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
                log.info("topic:"+topicPartition.topic()+"\t分区:"+topicPartition.partition()+"\t消费偏移量:"+offsets+"\tbegin偏移量:"+beginOffsets+"\tend偏移量:"+endOffsets);
                TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo();
                topicPartitionInfo.setTopic(topicPartition.topic());
                topicPartitionInfo.setOffsets(offsets);
                topicPartitionInfo.setPartition(topicPartition.partition());
                topicPartitionInfo.setBeginOffsets(beginOffsets);
                topicPartitionInfo.setEndOffsets(endOffsets);
                topicPartitionInfos.add(topicPartitionInfo);
            }
            log.info("查询分区信息结束...");
        }catch (Exception e){
            log.error("获取分区信息失败:{}",e.getMessage());
            e.printStackTrace();
        }finally {
            if(consumer!=null){
                consumer.close();
            }
        }
        return topicPartitionInfos;
    }

    public long countLag() {
        log.info("countLag开始...");
        List<TopicPartitionInfo> topicPartitionInfos = partitionInfos();
        int countLag = 0;
        for (TopicPartitionInfo topicPartitionInfo : topicPartitionInfos) {
            countLag += topicPartitionInfo.getEndOffsets()-topicPartitionInfo.getOffsets();
        }
        log.info("countLag结束...");
        return countLag;
    }

    public void seekToEnd(String topic, String groupId) {
        log.info("seekToEnd开始...");
        seek(STRATEGY_TO_END,null);
        log.info("seekToEnd结束...");
    }

    public void seekToBegin(String topic, String groupId) {
        log.info("seekToBegin开始...");
        seek(STRATEGY_TO_BEGIN,null);
        log.info("seekToBegin结束...");

    }

    public void seekToCustom(String topic, String groupId, Map<TopicPartition, Long> offsetsMap) {
        log.info("seekToCustom开始...");
        seek(STRATEGY_TO_CUSTOM, offsetsMap);
        log.info("seekToCustom结束...");
    }

    private void seek(String strategy, Map<TopicPartition, Long> offsetsMap){
        Consumer<String, String> consumer = null;
        try {
            Map<String, Object> properties = new HashMap();
            properties.put("enable.auto.commit",false);
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            Collection<TopicPartition> TopicPartitionList = transforTopicPartition(partitionInfoList);
            //分配分区 为获取当前消费值
            consumer.assign(TopicPartitionList);
            if(STRATEGY_TO_BEGIN.equals(strategy)){
                consumer.seekToBeginning(TopicPartitionList);
            }else if(STRATEGY_TO_END.equals(strategy)){
                consumer.seekToEnd(TopicPartitionList);
            }else if(STRATEGY_TO_CUSTOM.equals(strategy)){
                Consumer<String, String> finalConsumer = consumer;
                offsetsMap.forEach((TopicPartition topicPartition, Long offsets) ->{
                    finalConsumer.seek(topicPartition,offsets);
                });
            }

            for (TopicPartition topicPartition : TopicPartitionList) {
                consumer.position(topicPartition);
            }
            consumer.commitSync();
        }catch (Exception e){
            log.error("seek失败:{}",e.getMessage());
            e.printStackTrace();
        }finally {
            if(consumer!=null){
                consumer.close();
            }
        }
    }

    /**
     * 对象转换
     * @param list
     * @return
     */
    public Collection<TopicPartition> transforTopicPartition(List<PartitionInfo> list){
        List<TopicPartition> result = new ArrayList<TopicPartition>();
        if(list!=null){
            for(PartitionInfo info : list){
                result.add(new TopicPartition(info.topic(),info.partition()));
            }
        }
        return result;
    }


}
