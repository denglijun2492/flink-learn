package denglj.learn.flink.eventtime;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 间隔生成watermark
 * 该示例适用于以当前时间为参照物，在一定范围延迟(相当于不能跑历史数据)
 * Created by denglj on 2019/4/26.
 */
public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<String> {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    private final long maxTimeLag = 10*1000;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
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
