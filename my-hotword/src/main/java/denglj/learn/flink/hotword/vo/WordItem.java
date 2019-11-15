package denglj.learn.flink.hotword.vo;

/**
 * @Title
 * @Description: TODO
 * @Author denglj
 * @Date 2019/11/13 16:42
 **/
public class WordItem {
    private String type;
    private String word;
    private long count;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
