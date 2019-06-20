package denglj.learn.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by denglj on 2019/4/29.
 */
public class MyWordCountDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WordCount> dataStream = environment.socketTextStream("localhost", 9002)
                .flatMap(new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            WordCount w = new WordCount(s1, 1);
                            collector.collect(w);
                        }
                    }
                })
                .keyBy("word")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount t1, WordCount t2) throws Exception {
                        return new WordCount(t1.getWord(), t1.getCount() + t2.getCount());
                    }
                });

        dataStream.print();

        environment.execute();
    }
}
