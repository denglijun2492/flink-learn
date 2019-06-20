package denglj.learn.flink.kafka;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.kafka.consumer.MyConsumerMonitor;
import denglj.learn.flink.kafka.consumer.MyConsumerStat;
import denglj.learn.flink.kafka.consumer.MyPartitionMetric;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by denglj on 2019/5/30.
 */
public class KafkaMetricJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumeProperties = new Properties();
        consumeProperties.put("bootstrap.servers", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092");
        consumeProperties.put("group.id", "cons_denglj_metric");
//        consumeProperties.put("auto.offset.reset", "earliest");
        consumeProperties.put("enable.auto.commit", "true");
        consumeProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("denglj-topic-monitor", new SimpleStringSchema(), consumeProperties);

        DataStream<String> result = environment.addSource(consumer)
                .map(new MapFunction<String, MyPartitionMetric>() {
                    @Override
                    public MyPartitionMetric map(String value) throws Exception {
                        return JSON.parseObject(value, MyPartitionMetric.class);
                    }
                })
                .keyBy("topic")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60L), Time.seconds(5L)))
                .process(new ProcessWindowFunction<MyPartitionMetric, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<MyPartitionMetric> elements, Collector<String> out) throws Exception {
                        int total = 0;
                        Map<Integer, Long> lagMap = new HashMap<>();
                        long lag = 0;
                        MyPartitionMetric last = null;
                        for (MyPartitionMetric element : elements) {
                            total += element.getProcess();
                            last = element;
                            lagMap.put(element.getPartition(), element.getLag());
                        }
                        for (Long aLong : lagMap.values()) {
                            lag += aLong;
                        }
                        MyConsumerStat stat = new MyConsumerStat();
                        stat.setLag(lag);
                        stat.setTotal(total);
                        out.collect(JSON.toJSONString(stat));
                    }
                });

        result.print();

        environment.execute();

    }
}
