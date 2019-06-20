package denglj.learn.flink.rybd;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.rybd.source.RybdDbSource;
import denglj.learn.flink.vo.Gjxx;
import denglj.learn.flink.vo.Lgxx;
import denglj.learn.flink.vo.Ryxx;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by denglj on 2019/5/5.
 */
public class RybdDemo2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Ryxx> ryStream = environment.addSource(new RybdDbSource());
        DataStream<Lgxx> lgStream = environment.socketTextStream("localhost", 9000).map(new MapFunction<String, Lgxx>() {
            @Override
            public Lgxx map(String s) throws Exception {
                return JSON.parseObject(s, Lgxx.class);
            }
        });

        DataStream<Gjxx> gjxxStream = ryStream.coGroup(lgStream).
                where(new KeySelector<Ryxx, Object>() {
                    @Override
                    public Object getKey(Ryxx ryxx) throws Exception {
                        return ryxx.getSfzh();
                    }
                })
                .equalTo(new KeySelector<Lgxx, Object>() {
                    @Override
                    public Object getKey(Lgxx lgxx) throws Exception {
                        return lgxx.getSfzhm();
                    }
                })
                .window(GlobalWindows.create())
                .trigger(new Trigger<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Ryxx, Lgxx> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE;
                        //return element.isTwo() ? TriggerResult.FIRE_AND_PURGE :TriggerResult.FIRE;
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
                        System.out.println(window);
                    }
                })
                .evictor(new Evictor<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        for (Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> iterator = elements.iterator();iterator.hasNext();) {
                            TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>> next = iterator.next();
                            if(next.getValue().isTwo()){
                                iterator.remove();
                            }
                        }
                    }
                })
                .apply(new CoGroupFunction<Ryxx, Lgxx, Gjxx>() {
                    @Override
                    public void coGroup(Iterable<Ryxx> iterable, Iterable<Lgxx> iterable1, Collector<Gjxx> collector) throws Exception {
                        List<Ryxx> ryxxList = new ArrayList<>();
                        List<Lgxx> lgxxList = new ArrayList<>();
                        for(Iterator<Ryxx> iterator = iterable.iterator(); iterator.hasNext();){
                            ryxxList.add(iterator.next());
                        }
                        for(Iterator<Lgxx> iterator = iterable1.iterator(); iterator.hasNext();){
                            lgxxList.add(iterator.next());
                        }
                        System.out.println("ryxx:");
                        System.out.println(JSON.toJSONString(ryxxList));
                        System.out.println("lgxx:");
                        System.out.println(JSON.toJSONString(lgxxList));
                    }
                });

//        DataStream<Gjxx> gjxxStream = ryStream.join(lgStream).where(new KeySelector<Ryxx, Object>() {
//            @Override
//            public Object getKey(Ryxx ryxx) throws Exception {
//                return ryxx.getSfzh();
//            }
//        }).equalTo(new KeySelector<Lgxx, Object>() {
//            @Override
//            public Object getKey(Lgxx lgxx) throws Exception {
//                return lgxx.getSfzhm();
//            }
//        }).window(GlobalWindows.create())
//                .trigger(new Trigger<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>, GlobalWindow>() {
//                    @Override
//                    public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Ryxx, Lgxx> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.FIRE;
//                    }
//
//                    @Override
//                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
//
//                    }
//                })
//                .evictor(new Evictor<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>, GlobalWindow>() {
//                    @Override
//                    public void evictBefore(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
//
//                    }
//
//                    @Override
//                    public void evictAfter(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
//                        Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> iterator = elements.iterator();
//                        while(iterator.hasNext()){
//                        }
//                    }
//                })
//                .apply(new JoinFunction<Ryxx, Lgxx, Gjxx>() {
//                    @Override
//                    public Gjxx join(Ryxx ryxx, Lgxx lgxx) throws Exception {
//                        Gjxx gjxx = new Gjxx();
//                        gjxx.setSfzhm(ryxx.getSfzh());
//                        gjxx.setXm(ryxx.getXm());
//                        return gjxx;
//                    }
//                });

        DataStream<String> resultStream = gjxxStream.keyBy("sfzhm")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Gjxx, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Gjxx> elements, Collector<String> out) throws Exception {
                        int i=0;
                        Gjxx gj = null;
                        for (Gjxx element : elements) {
                            gj = element;
                            i++;
                        }
                        out.collect(gj.getSfzhm() + ":" + i);
                    }
                });


        resultStream.print();

        environment.execute();

    }
}
