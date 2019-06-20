package denglj.learn.flink.rybd.test;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by denglj on 2019/5/22.
 */
public class Test500WConsumer {
    public static void main(String[] args) throws Exception {
        StopWatch watch = new StopWatch();
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "192.168.120.120:9092");
        props.put("group.id", "cons_denglj_test");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("denglj_bdjg"));
        int i = 1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                if(i == 1){
                    watch.start();
                }
                if(i > 900){
                    watch.stop();
                    System.out.println("total : " + watch.getTime()/1000/60 );
                }
                System.out.println(i + " > " + record.value());
                i++;
            }
        }
    }
}
