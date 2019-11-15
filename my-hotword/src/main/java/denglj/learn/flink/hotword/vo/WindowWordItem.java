package denglj.learn.flink.hotword.vo;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 16:44
 **/
public class WindowWordItem {
    private String word;
    private long windowEnd;
    private long count;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
