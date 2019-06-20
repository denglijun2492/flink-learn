package denglj.learn.flink.rybd;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import javax.xml.transform.stream.StreamResult;
import java.util.*;

/**
 * Created by denglj on 2019/5/23.
 */
public class RyyjDemo {

    private static Logger log = LoggerFactory.getLogger(RyyjDemo.class);

    private static class MyRedisPeriodicWatermarks extends AbstractRichFunction implements AssignerWithPeriodicWatermarks<String>{
        private final long maxOutOfOrderness = 10 * 60 * 1000;
        private final long maxWatermarkTimeout = 1 * 60 * 1000;
        /**当前数据最大时间戳*/
        private long currentMaxTimestamp;
        /**上一个最大时间戳*/
        private long previousMaxTimestamp;
        /**用于判断从redis获取全局最大数据时间*/
        private long lastExtractTimestmp1 = System.currentTimeMillis();
        /**用于判断从redis获取最大窗口截止时间数据时间*/
        private long lastExtractTimestmp2 = System.currentTimeMillis();
        /**当前最大水标时间*/
        private long currentMaxWatermarkTime;

        private String initKey ;

        private long windowOffset;
        private long windowSize;

        public MyRedisPeriodicWatermarks(Time size){
            this.windowSize = size.toMilliseconds();
            this.windowOffset = 0;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            initKey = this.getRuntimeContext().getTaskNameWithSubtasks();
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Jedis jedis = new Jedis("172.16.104.5", 6379);
            //1.保存最大的数据时间和窗口时间到redis
            if(currentMaxTimestamp > previousMaxTimestamp){
                jedis.hset("flink:ryyj:bdjg", initKey, currentMaxTimestamp + "");
                long windowStartTime = TimeWindow.getWindowStartWithOffset(currentMaxTimestamp, windowOffset, windowSize);
                long windowEndTime = windowStartTime + windowSize;
                jedis.hset("flink:ryyj:bdjg-window", initKey, windowEndTime + "");
                previousMaxTimestamp = currentMaxTimestamp;
            }

            //2.当超过一定时间没有新数据进来就从redis获取最大时间，以生成水印(解决窗口不触发计算问题)
            long watermarkTime1 = 0;
            if(System.currentTimeMillis() - lastExtractTimestmp1 > 2*60*1000){
                Map<String, String> allTimestamp = jedis.hgetAll("flink:ryyj:bdjg");
                if(allTimestamp != null && allTimestamp.size() > 0){
                    for (String t : allTimestamp.values()) {
                        watermarkTime1 = Math.max(watermarkTime1, Long.valueOf(t));
                    }
                }
                lastExtractTimestmp1 = System.currentTimeMillis();
            }

            //3.当超过一定时间没有新数据进来就从redis获取最大窗口时间，以生成水印(解决最后数据不计算问题)
            long watermarkTime2 = 0;
            if(System.currentTimeMillis() - lastExtractTimestmp2 > 3*60*1000){
                Map<String, String> allTimestamp = jedis.hgetAll("flink:ryyj:bdjg-window");
                if(allTimestamp != null && allTimestamp.size() > 0){
                    for (String t : allTimestamp.values()) {
                        watermarkTime2 = Math.max(watermarkTime2, Long.valueOf(t));
                    }
                    watermarkTime2 = watermarkTime2 + maxOutOfOrderness + 1 ;
                }
                lastExtractTimestmp2 = System.currentTimeMillis();
            }

            //4.比较最大时间，返回
            currentMaxWatermarkTime = Math.max(currentMaxWatermarkTime, watermarkTime1);
            currentMaxWatermarkTime = Math.max(currentMaxWatermarkTime, watermarkTime2);
            currentMaxWatermarkTime = Math.max(currentMaxWatermarkTime, currentMaxTimestamp);

            return new Watermark(currentMaxWatermarkTime - maxOutOfOrderness);

        }
        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            try {
                Gjxx gjxx = JSON.parseObject(element, Gjxx.class);
                String hdfssj = gjxx.getHdfssj();
                long newTime = DateUtils.parseDate(hdfssj, "yyyyMMddHHmmss").getTime();
                currentMaxTimestamp = Math.max(newTime, currentMaxTimestamp);
                return newTime;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }finally {
                lastExtractTimestmp1 = System.currentTimeMillis();
                lastExtractTimestmp2 = System.currentTimeMillis();
            }
        }
    }

    private static class MyPeriodicWatermarks extends AbstractRichFunction implements AssignerWithPeriodicWatermarks<String>{
        private final long maxOutOfOrderness = 10 * 60 * 1000;
        private long currentMaxTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            log.info("watermark启动：" + System.currentTimeMillis());
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
//            log.info(this.getRuntimeContext().getTaskNameWithSubtasks());
            if (currentMaxTimestamp > 0) {
                long t = currentMaxTimestamp - maxOutOfOrderness;
                return new Watermark(t);
            } else {
                return null;
            }

        }
        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            try {
                Gjxx gjxx = JSON.parseObject(element, Gjxx.class);
                String hdfssj = gjxx.getHdfssj();
                long newTime = DateUtils.parseDate(hdfssj, "yyyyMMddHHmmss").getTime();
                currentMaxTimestamp = Math.max(newTime, currentMaxTimestamp);
                return newTime;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Configuration config = new Configuration();

        Properties consumeProperties = new Properties();
        consumeProperties.put("bootstrap.servers", "172.16.104.5:9092");
        consumeProperties.put("group.id", "cons-ryyj-demo2");
        consumeProperties.put("enable.auto.commit", "true");
        consumeProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> lgSource = new FlinkKafkaConsumer<>("test-bdjg", new SimpleStringSchema(), consumeProperties);

        DataStream<Gjxx> resultStream = environment.addSource(lgSource).setParallelism(1)
                .assignTimestampsAndWatermarks(new MyRedisPeriodicWatermarks(Time.minutes(2))).setParallelism(1)
                .map(new RichMapFunction<String, Gjxx>() {
                    @Override
                    public Gjxx map(String value) throws Exception {
                        Gjxx gjxx = JSON.parseObject(value, Gjxx.class);
                        return gjxx;
                    }
                })
                .keyBy("sfzhm")
                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
                .process(new ProcessWindowFunction<Gjxx, Gjxx, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Gjxx> elements, Collector<Gjxx> out) throws Exception {
                        List<Gjxx> gjxxs = new ArrayList();
                        for (Gjxx element : elements) {
                            gjxxs.add(element);
                        }
                        Collections.sort(gjxxs);
                        for (Gjxx gjxx : gjxxs) {
                            //预警业务处理
                            log.info(JSON.toJSONString(gjxx));
                            out.collect(gjxx);
                        }

                    }
                });

        resultStream.print();

        System.out.println("----------------------------");
        System.out.println(environment.getExecutionPlan());
        System.out.println("----------------------------");

        environment.execute();

    }
}
