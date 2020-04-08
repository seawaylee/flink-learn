package basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据源：1 2 3 4 5.....源源不断过来
 * 通过map打印一下接受到数据
 * 通过filter过滤一下数据，我们只需要偶数
 */
public class WriteTextDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> numberStream = env.addSource(new MyNoParalleSource()).setParallelism(1);
        SingleOutputStreamOperator<Long> dataStream = numberStream.map((MapFunction<Long, Long>) value -> {
            System.out.println("接受到了数据：" + value);
            return value;
        });
        SingleOutputStreamOperator<Long> filterDataStream = dataStream.filter((FilterFunction<Long>) number -> number % 2 == 0);

        // 没有集群的小伙伴也可以指定一个本地的路径，并写入一个文件中
        filterDataStream.writeAsText("./stream.log").setParallelism(1);
        env.execute("StreamingDemoWithMyNoPralalleSource");
    }
}