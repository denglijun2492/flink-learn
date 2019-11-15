package denglj.learn.flink.hotword;

import denglj.learn.flink.hotword.handle.*;
import denglj.learn.flink.hotword.vo.WordItem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Title 从kafka接收字符串分词、统计、排名，将结果输出到kafka
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 13:51
 **/
public class HotWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        Properties consuemProperties = KafkaConfiger.getConsuemProperties("denglj-cons-1");

        FlinkKafkaConsumer<String> wordSource = new FlinkKafkaConsumer<>("denglj-hotword", new SimpleStringSchema(), consuemProperties);

        SingleOutputStreamOperator<Object> stream = env.addSource(wordSource)
                .assignTimestampsAndWatermarks(new TimeAssigner())
                .flatMap(new WordSpliter())
                .keyBy("word")
                .timeWindow(Time.seconds(30), Time.seconds(5))
                .aggregate(new CountAgg(), new ResultFunction())
                .keyBy("windowEnd")
                .process(new TopNHotProcess(3));

        env.execute("Window WordCount");
    }

}
