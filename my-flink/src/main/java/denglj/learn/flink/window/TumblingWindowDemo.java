package denglj.learn.flink.window;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 基于处理时间的非叠加的时间窗口示例
 * 默认trigger是在窗口关闭时触发
 * Created by denglj on 2019/4/26.
 */
public class TumblingWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入格式：{"sfzhm":"111","hdfssj":"20190101000000"}
        DataStream<String> dataStream = environment.socketTextStream("localhost", 9000)
                .map(s -> JSON.parseObject(s, Gjxx.class))
                .keyBy("sfzhm")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .allowedLateness(Time.seconds(5))
                .apply(new WindowFunction<Gjxx, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Gjxx> input, Collector<String> out) throws Exception {
                        int i = 0;
                        String sfzhm = "";
                        for (Gjxx gjxx : input) {
                            i++;
                            sfzhm = gjxx.getSfzhm();
                        }
                        out.collect(sfzhm + "->" + i);
                    }
                });

        dataStream.print("TumblingWindowDemo");
        environment.execute();

    }
}
