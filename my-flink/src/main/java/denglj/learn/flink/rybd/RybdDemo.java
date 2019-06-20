package denglj.learn.flink.rybd;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.rybd.source.RybdRedisSource;
import denglj.learn.flink.vo.Gjxx;
import denglj.learn.flink.vo.Lgxx;
import denglj.learn.flink.vo.Ryxx;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;
import java.util.Properties;

/**
 * Created by denglj on 2019/5/5.
 */
public class RybdDemo {


    public static class MyTrigger extends Trigger<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>, GlobalWindow>{
        @Override
        public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Ryxx, Lgxx> element, long timestamp, GlobalWindow window, Trigger.TriggerContext ctx) throws Exception {
            return element.isOne() ? TriggerResult.CONTINUE : TriggerResult.FIRE;
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
    }

    public static class MyEvictor implements Evictor<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>, GlobalWindow> {
        @Override
        public void evictBefore(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            for(Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<Ryxx, Lgxx>>> iterator = elements.iterator(); iterator.hasNext();){
                CoGroupedStreams.TaggedUnion<Ryxx, Lgxx> value = iterator.next().getValue();
                if(value.isTwo()){
                    iterator.remove();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        DataStream<Ryxx> ryStream = environment.addSource(new RybdRedisSource());

        Properties consumeProperties = new Properties();
        consumeProperties.put("bootstrap.servers", "172.16.104.5:9092");
        consumeProperties.put("group.id", "cons-lgxx");
        consumeProperties.put("enable.auto.commit", "true");
        consumeProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> lgSource = new FlinkKafkaConsumer<>("test-lgxx", new SimpleStringSchema(), consumeProperties);
        DataStream<Lgxx> lgStream = environment.addSource(lgSource)
                .map(new MapFunction<String, Lgxx>() {
                    @Override
                    public Lgxx map(String value) throws Exception {
                        return JSON.parseObject(value, Lgxx.class);
                    }
        });

        DataStream<String> resultStream = ryStream.join(lgStream).where(new KeySelector<Ryxx, Object>() {
            @Override
            public Object getKey(Ryxx value) throws Exception {
                return value.getSfzh();
            }
        }).equalTo(new KeySelector<Lgxx, Object>() {
            @Override
            public Object getKey(Lgxx value) throws Exception {
                return value.getSfzhm();
            }
        }).window(GlobalWindows.create())
                .trigger(new MyTrigger())
                .evictor(new MyEvictor())
                .apply(new JoinFunction<Ryxx, Lgxx, String>() {
                    @Override
                    public String join(Ryxx first, Lgxx second) throws Exception {
                        Gjxx gjxx = new Gjxx();
                        gjxx.setSfzhm(first.getSfzh());
                        gjxx.setHdfssj(second.getRzsj());
                        gjxx.setHddd(second.getLgbh());
                        return JSON.toJSONString(gjxx);
                    }
                });


        Properties produceProperties = new Properties();
        produceProperties.put("bootstrap.servers", "172.16.104.5:9092");
//        produceProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        produceProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        resultStream.addSink(new FlinkKafkaProducer<String>("test-bdjg", new SimpleStringSchema(), produceProperties));

        resultStream.print();

        environment.execute();

    }
}
