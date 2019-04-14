package gjxx;

import com.alibaba.fastjson.JSON;
import gjxx.Gjxx;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by denglj on 2019/4/14.
 */
public class MyFlinkConsumer {
    public static void main(String[] args) throws Exception {
        //1.
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "my-consumer");
        DataStream<String > dstream = environment
            .addSource(new FlinkKafkaConsumer<>("test-flink1", new SimpleStringSchema(), properties))
            .map(new MapFunction<String, Gjxx>() {
                @Override
                public Gjxx map(String s) throws Exception {
                    return JSON.parseObject(s, Gjxx.class);
                }
            })
            .keyBy("sfzhm")
//                .timeWindow(Time.seconds(20))
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
            .apply(new WindowFunction<Gjxx, String, Tuple, TimeWindow>() {
                @Override
                public void apply(Tuple tuple, TimeWindow window, Iterable<Gjxx> input, Collector<String> out) throws Exception {
                    int i = 0;
                    for (Gjxx gjxx : input) {
                        i++;
                    }
                    out.collect(tuple + ":" + i+"");
                }
            });
        //3.
        dstream.print();
        //4.
        environment.execute();
    }
}
