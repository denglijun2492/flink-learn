package denglj.learn.flink.eventtime;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class AscendingTimestampDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        String path = "E:\\DragonWork\\git\\flink\\my-flink\\src\\main\\resources\\gjxx.txt";
        DataStream<String> dataStream = environment.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 100L);

//        DataStream<String> dataStream = environment.socketTextStream("localhost", 9001);
        DataStream<String> result = dataStream.map(new MapFunction<String, Gjxx>() {
            @Override
            public Gjxx map(String s) throws Exception {
                return JSON.parseObject(s, Gjxx.class);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Gjxx>(){
            @Override
            public long extractAscendingTimestamp(Gjxx element) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                try {
                    return sdf.parse(element.getHdfssj()).getTime();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }.withViolationHandler(new AscendingTimestampExtractor.FailingHandler()))
                .keyBy("sfzhm")
                .timeWindowAll(Time.seconds(10))
                .apply(new AllWindowFunction<Gjxx, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Gjxx> iterable, Collector<String> collector) throws Exception {
                        int i = 0;
                        String sfzhm = "";
                        for (Gjxx gjxx : iterable) {
                            i++;
                            sfzhm = gjxx.getSfzhm();
                        }
                        collector.collect(sfzhm + "->" + i);
                    }
                });

        result.print();

        environment.execute();

    }
}
