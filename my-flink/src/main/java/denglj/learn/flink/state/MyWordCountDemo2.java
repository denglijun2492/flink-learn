package denglj.learn.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Created by denglj on 2019/4/29.
 */
public class MyWordCountDemo2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<WordCount, Tuple> keyedStream1 = environment.socketTextStream("localhost", 9001)
                .flatMap(new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            WordCount w = new WordCount(s1, 1);
                            collector.collect(w);
                        }
                    }
                }).keyBy("word");

        KeyedStream<WordCount, Tuple> keyedStream2 = environment.socketTextStream("localhost", 9002)
                .flatMap(new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            WordCount w = new WordCount(s1, 1);
                            collector.collect(w);
                        }
                    }
                }).keyBy("word");

        DataStream<String> dataStream = keyedStream1.join(keyedStream2)
                .where(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wordCount) throws Exception {
                        return wordCount.getWord();
                    }
                })
                .equalTo(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wordCount) throws Exception {
                        return wordCount.getWord();
                    }
                })
                .window(GlobalWindows.create())
                .trigger(new Trigger<CoGroupedStreams.TaggedUnion<WordCount, WordCount>, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(CoGroupedStreams.TaggedUnion<WordCount, WordCount> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .apply(new JoinFunction<WordCount, WordCount, String>() {
                    @Override
                    public String join(WordCount wordCount, WordCount wordCount2) throws Exception {
                        return wordCount.toString() + wordCount2.toString();
                    }
                });

        dataStream.print();

        environment.execute();


    }
}
