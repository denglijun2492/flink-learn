package denglj.learn.flink.hotword.handle;

import denglj.learn.flink.hotword.vo.WordItem;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 16:35
 **/
public class CountAgg implements AggregateFunction<WordItem, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(WordItem value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
