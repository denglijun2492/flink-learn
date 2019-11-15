package denglj.learn.flink.hotword;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/14 13:46
 **/

public class SendMessageTests {
    @Test
    public void send() throws InterruptedException {
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);

        producer.send(new ProducerRecord("denglj-hotword", System.currentTimeMillis()+"", "denglj is"));

        Thread.sleep(300L);

        producer.close();

    }
}
