package denglj.learn.flink.eventtime;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.vo.Gjxx;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<String> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            String hdfssj = (String)JSON.parseObject(element).get("hdfssj");
            long newTime = sdf.parse(hdfssj).getTime();
            return newTime;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
