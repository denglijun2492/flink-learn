package denglj.learn.flink.rybd.test;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Ryxx;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by denglj on 2019/5/29.
 */
public class Test500WData {
    public static void main(String[] args) throws Exception{
        //1.
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "192.168.120.120:9092,192.168.120.121:9092,192.168.120.122:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);

        //2.
        StopWatch watch = new StopWatch();
        watch.start();
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.246:1521:orcl", "test_data", "dragon");
        PreparedStatement ps = connection.prepareStatement("select * from t_bs_rk_czrk_500w", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(1000);

        List<String> fields = null;
        ResultSet resultSet = ps.executeQuery();
        int k = 0;
        while (resultSet.next()){
            if(fields == null){
                fields = new ArrayList<>();
                int n = resultSet.getMetaData().getColumnCount();
                for(int i=1;i<=n;i++){
                    String fieldName = resultSet.getMetaData().getColumnName(i);
                    System.out.println(fieldName);
                    fields.add(fieldName);
                }
            }

            Map<String, String> row = new HashMap<>();
            for (String field : fields) {
                row.put(field, resultSet.getString(field));
            }
            producer.send(new ProducerRecord("ckry-500w", System.currentTimeMillis()+"", JSON.toJSONString(row)));
            System.out.println("发送数量："  +  ++k);
//            System.out.println(JSON.toJSONString(row));
        }
        watch.stop();
        System.out.println("总耗时：" + watch.getTime()/1000/60 + "分钟");

        Thread.sleep(10000L);
    }
}
