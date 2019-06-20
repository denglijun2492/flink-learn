package denglj.learn.flink.state;

/**
 * Created by denglj on 2019/4/24.
 */
public class WordCount {
    private String word;
    private int count;

    public WordCount(){}

    public WordCount(String word, int count){
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + ":" + count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
