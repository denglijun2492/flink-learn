package denglj.learn.flink.rybd.test;

import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by denglj on 2019/5/22.
 */
public class TestSendRedis {
    public static void main(String[] args) throws Exception{
        /*Map<String, String> rys = new HashMap<>();
        rys.put("111", "1");
        rys.put("222", "1");
        rys.put("333", "1");

        Jedis jedis = new Jedis("172.16.104.5", 6379);

        jedis.hmset("rys", rys);*/


        Jedis jedis = new Jedis("172.16.104.5", 6379);
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.246:1521:orcl", "test_data", "dragon");
        PreparedStatement ps = connection.prepareStatement("select * from (select * from t_bs_rk_czrk_500w order by dbms_random.random) where rownum < 1000", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(500);

        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            String sfzh = resultSet.getString("SFZH");
            jedis.hset("yjbd-500w", sfzh, "1");
        }
    }
}
