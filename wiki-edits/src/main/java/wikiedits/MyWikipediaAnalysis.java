package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import javax.xml.crypto.Data;

public class MyWikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.构建输入源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        //3.指定分组
        KeyedStream<WikipediaEditEvent, String> keyedStream = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent event) throws Exception {
                return event.getUser();
            }
        });

        //4.每10秒按用户统计编辑的字节数
        DataStream<Tuple2<String, Long>> result = keyedStream
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) throws Exception {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });

        //打印到控制台
//        result.print();

        result.map(new MapFunction<Tuple2<String,Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                return value.toString();
            }
        }).addSink(new FlinkKafkaProducer<>("192.168.1.11:9092","wiki-result", new SimpleStringSchema()));

        //执行
        see.execute();


    }
}
