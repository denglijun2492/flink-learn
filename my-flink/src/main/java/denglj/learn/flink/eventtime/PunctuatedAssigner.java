package denglj.learn.flink.eventtime;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 每来一条数据都可以计算watermark，区别于间隔生成的方式
 * Created by denglj on 2019/4/26.
 */
public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<String> {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        try {
            String hdfssj = (String) JSON.parseObject(element).get("hdfssj");
            long newTime = sdf.parse(hdfssj).getTime();
            return newTime;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
