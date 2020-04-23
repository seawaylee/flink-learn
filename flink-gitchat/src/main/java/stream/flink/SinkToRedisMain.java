package stream.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import stream.dto.ProductEvent;
import stream.utils.KafkaConfigUtil;
import stream.utils.ProductUtil;

import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-04-22 18:14
 */
public class SinkToRedisMain {
    private static FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = KafkaConfigUtil.buildKafkaProps();
        SingleOutputStreamOperator<Tuple2<String, String>> dataStream = env.addSource(new FlinkKafkaConsumer<>(
                ProductUtil.topic,
                new SimpleStringSchema(),
                props))
                .map(str -> JSON.parseObject(str, ProductEvent.class))
                .flatMap(new FlatMapFunction<ProductEvent, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(ProductEvent productEvent, Collector<Tuple2<String, String>> collector) throws Exception {
                        collector.collect(new Tuple2<>(productEvent.getId().toString(), productEvent.getPrice().toString()));
                    }
                });
        //dataStream.print();
        dataStream.addSink(new RedisSink<>(conf, new RedisSinkMapper()));
        env.execute("flink redis connector");
    }

    private static class RedisSinkMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "product");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
