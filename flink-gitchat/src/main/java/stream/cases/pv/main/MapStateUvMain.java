package stream.cases.pv.main;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import stream.cases.pv.dto.UserVisitWebEvent;
import stream.cases.pv.util.PvKafkaConfig;

/**
 * 使用MapState状态存储接收到的数据
 *
 * @author SeawayLee
 * @create 2020-05-26 16:17
 */
public class MapStateUvMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                PvKafkaConfig.TOPIC,
                new SimpleStringSchema(),
                PvKafkaConfig.getConsumerProperties("pv-stat-group"))
                .setStartFromLatest();

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        env.addSource(kafkaConsumer)
                .map(s -> JSON.parseObject(s, UserVisitWebEvent.class))
                .keyBy("date", "pageId")
                .map(new RichMapFunction<UserVisitWebEvent, Tuple2<String, Long>>() {
                    private MapState<String, Boolean> userIdState;
                    private ValueState<Long> uvState;

                    @Override
                    public Tuple2<String, Long> map(UserVisitWebEvent event) throws Exception {
                        if (uvState.value() == null) {
                            uvState.update(0L);
                        }
                        if (!userIdState.contains(event.getUserId())) {
                            userIdState.put(event.getUserId(), null);
                            uvState.update(uvState.value() + 1);
                        }
                        String redisKey = event.getDate() + "_" + event.getPageId();
                        System.out.println(redisKey + "   :::   " + uvState.value());
                        return Tuple2.of(redisKey, uvState.value());
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        userIdState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("userIdState",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Boolean>() {
                                        })));
                        uvState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("uvState",
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })));
                    }
                })
                .addSink(new RedisSink<>(config, new RedisMapper<Tuple2<String, Long>>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.SET);
                    }

                    @Override
                    public String getKeyFromData(Tuple2<String, Long> data) {
                        return data.f0;
                    }

                    @Override
                    public String getValueFromData(Tuple2<String, Long> data) {
                        return data.f1.toString();
                    }
                }));
        env.execute("Redis Set UV Stat");
    }
}
