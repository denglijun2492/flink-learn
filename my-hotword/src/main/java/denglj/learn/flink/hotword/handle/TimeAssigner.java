package denglj.learn.flink.hotword.handle;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 15:02
 **/
public class TimeAssigner implements AssignerWithPeriodicWatermarks<String> {

    private final long maxTimeLag = 2000;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        return System.currentTimeMillis();
    }
}
