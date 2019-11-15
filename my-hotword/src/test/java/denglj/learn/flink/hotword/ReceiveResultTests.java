package denglj.learn.flink.hotword;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import denglj.learn.flink.hotword.handle.KafkaConfiger;
import denglj.learn.flink.hotword.vo.WindowWordItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/14 14:14
 **/
public class ReceiveResultTests {
    @Test
    public void receive(){
        Properties consuemProperties = KafkaConfiger.getConsuemProperties("denglj-cons-1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(consuemProperties);
        consumer.subscribe(Arrays.asList("denglj-hotword-result"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                List<WindowWordItem> windowWordItems = JSONArray.parseArray(record.value(), WindowWordItem.class);
                System.out.println("实时排名： " + windowWordItems.get(0).getWindowEnd());
                for (WindowWordItem item : windowWordItems) {
                    System.out.println(String.format("%s -> %s", item.getWord(), item.getCount()));
                }
                System.out.println("\n");
            }
        }

    }
}
