package denglj.learn.flink.hotword.handle;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/14 14:06
 **/
public class KafkaConfiger {
    public static Properties getConsuemProperties(String group){
        Properties consumeProperties = new Properties();
        consumeProperties.put("bootstrap.servers", "192.168.120.120:9092");
        consumeProperties.put("group.id", group);
        consumeProperties.put("enable.auto.commit", "true");
        consumeProperties.put("auto.offset.reset", "earliest");
        consumeProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return consumeProperties;
    }

    public static KafkaProducer getProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.120.120:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }
}
