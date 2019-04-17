package gjxx;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SocketGjxx {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = environment.socketTextStream("localhost", 9001, "\n")
                .map(new MapFunction<String, Gjxx>() {
                    @Override
                    public Gjxx map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        Gjxx gjxx = new Gjxx();
                        gjxx.setHdfssj(arr[2]);
                        gjxx.setSfzhm(arr[1]);
                        gjxx.setXm(arr[0]);
                        return gjxx;
                    }
                })
                .keyBy("sfzhm")

                .window(GlobalWindows.create())

                .trigger(new MyTrigger())
                .evictor(new MyEvictor())
                .process(new ProcessWindowFunction<Gjxx, String, Tuple, GlobalWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Gjxx> elements, Collector<String> out) throws Exception {
                        int i = 0;
                        String s = "";
                        for (Gjxx element : elements) {
                            s+= element.getHdfssj()+",";
                            i++;
                        }
                        out.collect(tuple + ":" + i + ":" + s);
                    }
                });
        ds.print();

        environment.execute();
    }
}
