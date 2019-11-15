package denglj.learn.flink.hotword.handle;/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/14 13:35
 **/

import denglj.learn.flink.hotword.vo.WordItem;
import javafx.application.Application;
import javafx.stage.Stage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class WordSpliter implements FlatMapFunction<String, WordItem> {
    @Override
    public void flatMap(String sentence, Collector<WordItem> out) throws Exception {
        for (String word: sentence.split(" ")) {
            WordItem item = new WordItem();
            item.setCount(1);
            item.setWord(word);
            out.collect(item);
        }
    }
}
