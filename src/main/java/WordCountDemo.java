import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口
 * 每隔1秒钟统计最近2秒内的数据，打印到控制台。
 *
 * @author SeawayLee
 * @create 2020-03-25 17:07
 */
public class WordCountDemo {
    public static void main(String[] args) throws Exception {
        // nc -l 9000
        String hostname = "127.0.0.1";
        int port = 9000;

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStream<String> dataStream = env.socketTextStream(hostname, port);
        // 定义逻辑操作
        DataStream<WordCount> wordAndOneStream = dataStream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> out) {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        });
        // 执行算子
        DataStream<WordCount> resultStream = wordAndOneStream.keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
        // 打印结果
        resultStream.print();
        // 启动任务
        env.execute("WindowWordCountJava");


    }

    public static class WordCount {
        public String word;
        public long count;

        //记得要有这个空构造方法
        public WordCount() {

        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
