package stream.cases.uniq;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import stream.cases.pv.dto.UserVisitWebEvent;
import stream.utils.KafkaUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * @author SeawayLee
 * @create 2020-06-17 15:37
 */
public class KeyedStateDeduplication {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        RocksDBStateBackend rocksDbStateBackend = new RocksDBStateBackend("hdfs:///flink/checkpoints", true);
        rocksDbStateBackend.setNumberOfTransferThreads(3);
        rocksDbStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        rocksDbStateBackend.isIncrementalCheckpointsEnabled();

        env.setStateBackend(rocksDbStateBackend);

        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(10));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8));
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(20));
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "keyed-state-deduplication");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("dedup.test.topic", new SimpleStringSchema(), properties);

        env.addSource(kafkaConsumer)
                .map(log -> JSON.parseObject(log, UserVisitWebEvent.class))
                .keyBy((KeySelector<UserVisitWebEvent, String>) UserVisitWebEvent::getId)
                .addSink(new RichSinkFunction<UserVisitWebEvent>() {
                    private ValueState<Boolean> isExist;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> keyedStateDeduplicated = new ValueStateDescriptor<>("KeyedStateDeduplication", TypeInformation.of(new TypeHint<Boolean>() {
                        }));
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(36))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .cleanupInRocksdbCompactFilter(50000000L)
                                .build();
                        keyedStateDeduplicated.enableTimeToLive(ttlConfig);
                        isExist = getRuntimeContext().getState(keyedStateDeduplicated);
                    }

                    @Override
                    public void invoke(UserVisitWebEvent value, Context context) throws Exception {
                        if (null == isExist.value()) {
                            isExist.update(true);
                        } else {

                        }
                    }
                });

    }
}
