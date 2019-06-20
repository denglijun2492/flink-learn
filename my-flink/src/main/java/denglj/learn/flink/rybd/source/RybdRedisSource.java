package denglj.learn.flink.rybd.source;

import denglj.learn.flink.vo.Ryxx;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Created by denglj on 2019/5/5.
 */
public class RybdRedisSource extends RichSourceFunction<Ryxx> {


    private Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = new Jedis("172.16.104.5", 6379);
    }

    @Override
    public void run(SourceContext<Ryxx> ctx) throws Exception {
        load(ctx);
        while (true){
            Thread.sleep(50000L);
        }

    }

    private void load(SourceContext<Ryxx> ctx) throws Exception{
        Map<String, String> rys = (Map<String, String>) jedis.hgetAll("rys");
        for (String sfz : rys.keySet()) {
            Ryxx ry = new Ryxx();
            ry.setSfzh(sfz);
            ctx.collect(ry);
        }
    }

    @Override
    public void cancel() {

    }
}
