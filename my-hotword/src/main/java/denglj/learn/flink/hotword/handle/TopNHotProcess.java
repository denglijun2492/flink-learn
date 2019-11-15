package denglj.learn.flink.hotword.handle;

import com.alibaba.fastjson.JSON;
import denglj.learn.flink.hotword.vo.WindowWordItem;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 17:03
 **/
public class TopNHotProcess extends KeyedProcessFunction<Tuple, WindowWordItem, Object> {

    private final int topSize;

    private ListState<WindowWordItem> itemState;

    public TopNHotProcess(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor itemsStateDesc = new ListStateDescriptor<>(
                "itemState-state",
                WindowWordItem.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(WindowWordItem value, Context ctx, Collector<Object> out) throws Exception {
        itemState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        List<WindowWordItem> allItems = new ArrayList();
        for (WindowWordItem item : itemState.get()) {
            allItems.add(item);
        }

        itemState.clear();
        allItems.sort(new Comparator<WindowWordItem>() {
            @Override
            public int compare(WindowWordItem o1, WindowWordItem o2) {
                return (int)(o2.getCount() - o1.getCount());
            }
        });

        List<WindowWordItem> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("====================================\n");
        sb.append("时间: ").append(timestamp).append("\n");
        for (int i = 0; i < allItems.size() && result.size()<topSize ; i++) {
            WindowWordItem item = allItems.get(i);
            result.add(item);
            sb.append(String.format("%s -> %s", item.getWord(), item.getCount())).append("\n");
        }
        System.out.println(sb.toString());
        KafkaConfiger.getProducer().send(
                new ProducerRecord(
                        "denglj-hotword-result",
                        System.currentTimeMillis()+"",
                        JSON.toJSONString(result))
        );
    }
}
