package basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingDemoWithMyPralalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 这个代码.setParallelism(2)设置了并行2
        DataStreamSource<Long> numberStream = env.addSource(new MyParalleSource()).setParallelism(10);
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