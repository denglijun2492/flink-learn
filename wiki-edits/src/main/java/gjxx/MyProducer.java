package gjxx;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by denglj on 2019/4/14.
 */
public class MyProducer {
    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);
        for (int i = 0; i <20 ; i++) {
            Gjxx gjxx = new Gjxx();
            gjxx.setHdfssj("2019040307202");
            gjxx.setXm("邓礼俊");
            gjxx.setSfzhm("88888" + i%3);
            producer.send(new ProducerRecord("test-flink1", JSON.toJSONString(gjxx)));
//            producer.send(new ProducerRecord("test-flink1", JSON.toJSONString(gjxx).getBytes("utf-8")));
        }
        Thread.sleep(300L);
        producer.close();

    }


}
