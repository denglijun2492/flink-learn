package denglj.learn.flink.window;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * keyby后每个key一个窗口示例，与时间无关
 * Created by denglj on 2019/4/26.
 */
public class GlobalWindowDemo {

    private static Logger log = LoggerFactory.getLogger(GlobalWindowDemo.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入格式：{"sfzhm":"111","hdfssj":"20190101000000"}
        DataStream<String> dataStream = environment.socketTextStream("localhost", 9000)
                .map(s -> JSON.parseObject(s, Gjxx.class))
                .keyBy("sfzhm")
                .window(GlobalWindows.create())
                .trigger(new MyTrigger())
//                .evictor(new MyEvictor())
                .evictor(CountEvictor.of(10, true))
                .apply(new MyWindowFunction());

        dataStream.print("GlobalWindowDemo");
        environment.execute();

    }

    public static class MyTrigger extends Trigger{
        /**
         * 添加每一个元素时触发窗口计算
         */
        @Override
        public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
            //TriggerResult.FIRE 触发计算
            //TriggerResult.CONTINUE 什么也不做
            //TriggerResult.PURGE 清理窗口元素
            //TriggerResult.FIRE_AND_PURGE 触发计算和清理
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {
            log.info("on clear...");
        }
    }

    public static class MyWindowFunction implements WindowFunction<Gjxx, String, Tuple, GlobalWindow>{
        @Override
        public void apply(Tuple tuple, GlobalWindow window, Iterable<Gjxx> input, Collector<String> out) throws Exception {
            int i = 0;
            String sfzhm = "";
            for (Gjxx gjxx : input) {
                i++;
                sfzhm = gjxx.getSfzhm();
            }
            out.collect(sfzhm + "->" + i);
        }
    }

    /**
     * 驱逐窗口元素
     */
    public static class MyEvictor implements Evictor<Gjxx, GlobalWindow>{
        @Override
        public void evictBefore(Iterable<TimestampedValue<Gjxx>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Gjxx>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            for(Iterator<TimestampedValue<Gjxx>> iterator = elements.iterator(); iterator.hasNext();){
                Gjxx gjxx = iterator.next().getValue();
                if(gjxx.getSfzhm().equals("111")){
                    iterator.remove();
                }
            }
        }
    }
}
