package basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：从自定义的数据数据源里面获取数据，然后过滤出偶数
 */
public class StreamingDemoWithMyNoPralalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 接收数据源
        DataStreamSource<Long> numberStream = env.addSource(new MyNoParalleSource()).setParallelism(1);
        SingleOutputStreamOperator<Long> dataStream = numberStream
                .map((MapFunction<Long, Long>) value -> {
                    System.out.println("接受到了数据：" + value);
                    return value;
                })
                .filter((FilterFunction<Long>) number -> number % 2 == 0);

        dataStream.print().setParallelism(1);
        env.execute("StreamingDemoWithMyNoPralalleSource");
    }
}