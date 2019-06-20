package denglj.learn.flink.rybd.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import denglj.learn.flink.vo.Gjxx;
import denglj.learn.flink.vo.Lgxx;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by denglj on 2019/5/22.
 */
public class TestSendData {
    public static void main(String[] args) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);

        /*
        Lgxx lgxx = new Lgxx();
        lgxx.setLgbh("L002");
        lgxx.setRzsj("20190101080010");
        lgxx.setSfzhm("222");
        String info = JSON.toJSONString(lgxx);

        producer.send(new ProducerRecord("test-lgxx", System.currentTimeMillis()+"", info));
        */
        Gjxx gjxx = new Gjxx();
        gjxx.setHddd("L001");;
        gjxx.setHdfssj("20190102181504");
        gjxx.setSfzhm("333");
        gjxx.setXm("");

        for (int i = 0; i < 10000 ; i++) {
            producer.send(new ProducerRecord("test-bdjg", System.currentTimeMillis()+"", JSON.toJSONString(gjxx)));
            System.out.println(i);
        }
        Thread.sleep(300L);
        producer.close();
    }
}
