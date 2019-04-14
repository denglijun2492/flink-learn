package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        //1.获取port
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        int port = parameterTool.getInt("port");
        //2.获取execution enviroment
        StreamExecutionEnvironment sse = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.获取输入流
        DataStreamSource<String> text = sse.socketTextStream("localhost", 9000, "\n");
        //4.按空格拆分词，分组 窗口统计
        DataStream<WordWithCount> windowCount = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        for (String s : value.split("\\s")) {
                            out.collect(new WordWithCount(s, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(10L))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count+b.count);
                    }
                });
        //5.打印
        windowCount.print().setParallelism(1);

        sse.execute("socket window word count....");
    }
}
