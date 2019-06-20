package denglj.learn.flink.eventtime;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import denglj.learn.flink.window.GlobalWindowDemo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by denglj on 2019/4/26.
 */
public class EventTimeWatermarkDemo {
    private static Logger log = LoggerFactory.getLogger(GlobalWindowDemo.class);

    public static void main(String[] args) throws Exception {

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //输入格式：{"sfzhm":"111","hdfssj":"20190101000000"}
        DataStream<String> dataStream = environment.socketTextStream("localhost", 9000)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
//                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                .map(s -> JSON.parseObject(s, Gjxx.class))
                .keyBy("sfzhm")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Gjxx, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Gjxx> elements, Collector<String> out) throws Exception {
                        String s = "";
                        String sfzhm =  "";
                        for (Gjxx gjxx : elements) {
                            s += gjxx.getHdfssj() + ",";
                            sfzhm = gjxx.getSfzhm();
                        }
                        String t1 = sdf.format(new Date(context.window().getStart()));
                        String t2 = sdf.format(new Date(context.window().getEnd()));
                        log.info("window range：{}-{}", t1, t2);
                        out.collect(sfzhm + "->" + s);
                    }
                });

        dataStream.print("EventTimeWatermarkDemo");
        environment.execute();

    }

}
