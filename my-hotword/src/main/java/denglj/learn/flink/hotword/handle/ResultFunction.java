package denglj.learn.flink.hotword.handle;

import denglj.learn.flink.hotword.vo.WindowWordItem;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 16:39
 **/
public class ResultFunction implements WindowFunction<Long, WindowWordItem, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple s,
                      TimeWindow window,
                      Iterable<Long> input,
                      Collector<WindowWordItem> out) throws Exception {
        WindowWordItem item = new WindowWordItem();
        item.setCount(input.iterator().next());
        item.setWindowEnd(window.getEnd());
        item.setWord(s.getField(0));
        out.collect(item);
    }
}
