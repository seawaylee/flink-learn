package demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author SeawayLee
 * @create 2020-04-09 11:23
 */
public class SocketStreamMain {
    public static void main(String[] args) throws Exception {
        // nc -l 9000
        String hostname = "127.0.0.1";
        Integer port = 9000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumRes = sourceStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = line.split("\\W+");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);
        sumRes.print();
        env.execute("Java Word Count from SocketText");
    }
}
