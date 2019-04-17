package gjxx;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by denglj on 2019/4/14.
 */
public class MyFlinkConsumerFile {
    public static void main(String[] args) throws Exception {
        //1.
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.
        String filePath = "E:\\DragonWork\\git\\flink\\wiki-edits\\src\\main\\resources\\gjxx.txt";
        DataStream<String > dstream = environment.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 200L)
            .map(new MapFunction<String, Gjxx>() {
                @Override
                public Gjxx map(String s) throws Exception {
                    System.out.println(s);
                    return JSON.parseObject(s, Gjxx.class);
                }
            })
            .keyBy("sfzhm")
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
        String outPath = "E:\\DragonWork\\git\\flink\\wiki-edits\\src\\main\\resources\\result";
//        dstream.writeAsText(outPath, FileSystem.WriteMode.OVERWRITE);
        dstream.print();
        //4.
        environment.execute();
    }
}
