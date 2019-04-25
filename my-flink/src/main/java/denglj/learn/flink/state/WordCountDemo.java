package denglj.learn.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 统计单词数示例
 * Created by denglj on 2019/4/24.
 */
public class WordCountDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //启用检查点，每5秒进行保存
        environment.enableCheckpointing(5000);
        //检查点模式，默认EXACTLY_ONCE
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //检查点保存超时时间
        environment.getCheckpointConfig().setCheckpointTimeout(60000);
        //最小保存间隔
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //最大并发检查点线程数
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置状态后端存储，一般通过flink.conf配置
//        StateBackend stateBackend = new FsStateBackend("file:///opt/tmp/flink");
//        environment.setStateBackend(stateBackend);
        //配置策略为cancel后保留
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment.getCheckpointConfig().setFailOnCheckpointingErrors(true);


        DataStream<WordCount> dataStream = environment.socketTextStream("172.16.104.5", 9000)
                .flatMap(new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            WordCount wordCount = new WordCount();
                            wordCount.setCount(1);
                            wordCount.setWord(s1);
                            collector.collect(wordCount);
                        }
                    }
                })
                .keyBy("word")
                .sum("count");

        dataStream.print("result");

        environment.execute("countDemo");
    }
}
