package basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectionDemo {
    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        //注意：针对此source，并行度只能设置为1
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<String> text2_str = text2.map((MapFunction<Long, String>) value -> {
            // 这里是第二个数据源，字符串我加了一个前缀str_
            return "str_" + value;
        });
        ConnectedStreams<Long, String> connectStream = text1.connect(text2_str);
        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                // 在这里可以进行业务处理
                return value;
            }
            @Override
            public Object map2(String value) throws Exception {
                // 在这里也可以进行业务处理
                return value;
            }
        });

        //打印结果
        result.print().setParallelism(2);
        String jobName = ConnectionDemo.class.getSimpleName();
        env.execute(jobName);
    }
}