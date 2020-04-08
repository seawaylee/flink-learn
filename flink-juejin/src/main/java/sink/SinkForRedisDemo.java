package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 把数据写入redis
 *
 * @author SeawayLee
 * @create 2020-03-27 15:05
 */
public class SinkForRedisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textSource = env.socketTextStream("127.0.0.1", 9000, "\n");
        // 对数据进行组装,把string转化为tuple2<String,String>
        SingleOutputStreamOperator<Tuple2<String, String>> listWordsData = textSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("l_words", value);
            }
        });
        // 创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(6379).build();
        // 创建redissink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new RedisMapper<Tuple2<String, String>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                // 表示从接收的数据中获取需要操作的redis key
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                // 表示从接收的数据中获取需要操作的redis value
                return data.f1;
            }
        });

        listWordsData.addSink(redisSink);
        env.execute("StreamingDemoToRedis");
    }
}
