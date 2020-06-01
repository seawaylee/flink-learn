package stream.cases.pv.main;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import stream.cases.pv.util.PvKafkaConfig;
import stream.cases.pv.dto.UserVisitWebEvent;

import java.util.Properties;

/**
 * 使用Redis数据结构统计UV
 *
 * @author SeawayLee
 * @create 2020-05-26 15:53
 */
public class RedisSetUvMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = PvKafkaConfig.getConsumerProperties("app-uv-stat");
        FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer<>(PvKafkaConfig.TOPIC, new SimpleStringSchema(), properties)
                .setStartFromLatest();
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        env.addSource(kafkaConsumer).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String data) throws Exception {
                UserVisitWebEvent event = JSON.parseObject(data, UserVisitWebEvent.class);
                String redisKey = event.getDate() + "_" + event.getPageId();
                return new Tuple2<>(redisKey, event.getUserId());
            }
        }).addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SADD);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data.f1;
            }
        }));
        env.execute("Redis Set Uv Stat");
    }
}
