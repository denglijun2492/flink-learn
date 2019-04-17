package gjxx;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

public class MyEvictor implements Evictor {
    @Override
    public void evictBefore(Iterable elements, int size, Window window, EvictorContext evictorContext) {

    }

    @Override
    public void evictAfter(Iterable elements, int size, Window window, EvictorContext evictorContext) {
        for (Object element : elements) {
            TimestampedValue tv = (TimestampedValue)element;
            System.out.println(((Gjxx)tv.getValue()).getHdfssj());
        }
    }
}
