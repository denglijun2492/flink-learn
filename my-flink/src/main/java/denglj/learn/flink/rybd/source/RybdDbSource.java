package denglj.learn.flink.rybd.source;

import denglj.learn.flink.vo.Ryxx;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by denglj on 2019/5/5.
 */
public class RybdDbSource extends RichSourceFunction<Ryxx> {

    public static void main(String[] args) throws Exception {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(1546405191000L);
        Date date = DateUtils.parseDate("20190101000000", "yyyyMMddHHmmss");
        System.out.println(date.getTime());
        System.out.println(DateFormatUtils.format(1546272060000L, "yyyyMMddHHmmss"));
    }

    private Connection connection = null;

    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("oracle.jdbc.driver.OracleDriver");
        connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.246:1521:orcl", "zdr_train", "dragon");
        ps = connection.prepareStatement("select * from T_RY_JBXX where sfzh in('451026198511278974','451026198511278977')", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(1000);

    }

    @Override
    public void run(SourceContext<Ryxx> ctx) throws Exception {
        load(ctx);
        while (true){
            Thread.sleep(500000L);
        }

    }

    private void load(SourceContext<Ryxx> ctx) throws Exception{
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            int sfzh = resultSet.findColumn("SFZH");
            int xm = resultSet.findColumn("XM");
            Ryxx ry = new Ryxx();
            ry.setSfzh(resultSet.getString(sfzh));
            ry.setXm(resultSet.getString(xm));
            ctx.collect(ry);
        }
    }

    @Override
    public void cancel() {
        try {
            if(ps != null){
                ps.close();
            }
            if(connection != null){
                connection.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
