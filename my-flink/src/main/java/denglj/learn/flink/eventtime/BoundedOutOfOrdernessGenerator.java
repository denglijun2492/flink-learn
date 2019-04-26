package denglj.learn.flink.eventtime;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 间隔生成watermark
 * 该示例适用于当数据无序到来，并在一定的延迟时间范围内(10秒)
 * Created by denglj on 2019/4/26.
 */
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<String> {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    private final long maxOutOfOrderness = 10*1000;

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        try {
            String hdfssj = (String) JSON.parseObject(element).get("hdfssj");
            long newTime = sdf.parse(hdfssj).getTime();
            currentMaxTimestamp = Math.max(newTime, currentMaxTimestamp);
            return newTime;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
