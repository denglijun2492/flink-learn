package denglj.learn.flink.rybd;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.kafka.consumer.MyFlinkKafkaConsumer;
import denglj.learn.flink.vo.Lgxx;
import denglj.learn.flink.vo.Ryxx;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Created by denglj on 2019/5/29.
 */
public class Rybd500wDemo {

    private static Map<String, String> bdRys;

    public static class Context{
        private static ThreadLocal<Jedis> threadLocal = new ThreadLocal<>();

        public static Jedis get(){
            if(threadLocal.get() == null){
                Jedis jedis = new Jedis("172.16.104.5", 6379);
                threadLocal.set(jedis);
            }
            return threadLocal.get();
        }
    }

    public static class MyFilter1 extends RichFilterFunction<String>{

        @Override
        public boolean filter(String value) throws Exception {
            Map<String, String> row = JSON.parseObject(value, HashMap.class);
            Lgxx lgxx = new Lgxx();
            lgxx.setSfzhm(row.get("SFZH"));
            lgxx.setRzsj("ZHSJC");

            if(bdRys == null){

                bdRys = Context.get().hgetAll("yjbd-500w");
            }
            if(bdRys.containsKey(lgxx.getSfzhm())){
                return true;
            }else{
                return false;
            }
        }

    }

    public static class MyFilter2 extends RichFilterFunction<String>{

        @Override
        public boolean filter(String value) throws Exception {
            Map<String, String> row = JSON.parseObject(value, HashMap.class);
            Lgxx lgxx = new Lgxx();
            lgxx.setSfzhm(row.get("SFZH"));
            lgxx.setRzsj("ZHSJC");

            String v = (String)Context.get().hget("yjbd-500w", lgxx.getSfzhm());
            if(StringUtils.isNotBlank(v)){
                return true;
            }else{
                return false;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String filterType = parameterTool.get("type");

        String groupId = parameterTool.get("groupId");

        System.out.println("比对运行模式：" + filterType);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties consumeProperties = new Properties();
        consumeProperties.put("bootstrap.servers", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092");
        consumeProperties.put("group.id", groupId);//cons_500w_ck
        consumeProperties.put("auto.offset.reset", "earliest");
        consumeProperties.put("enable.auto.commit", "true");
        consumeProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        MyFlinkKafkaConsumer<String> lgSource = new MyFlinkKafkaConsumer<>("denglj_500w", new SimpleStringSchema(), consumeProperties);
//        lgSource.setStartFromTimestamp(System.currentTimeMillis() - 24*60*60*1000);


        FilterFunction filterFunction = filterType.equals("local") ? new MyFilter1() : new MyFilter2();
        DataStream<String> resultStream = environment.addSource(lgSource).filter(filterFunction);
        resultStream.print();

        Properties produceProperties = new Properties();
        produceProperties.put("bootstrap.servers", "192.168.120.120:9092");
//        produceProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        produceProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        resultStream.addSink(new FlinkKafkaProducer<String>("denglj_bdjg", new SimpleStringSchema(), produceProperties));

        environment.execute();

    }
}
