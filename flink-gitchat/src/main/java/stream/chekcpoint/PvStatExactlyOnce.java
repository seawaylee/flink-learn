package stream.chekcpoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.api.java.functions.KeySelector;

import stream.chekcpoint.util.PvStatExactlyOnceKafkaUtil;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author SeawayLee
 * @create 2020-05-12 14:50
 */
public class PvStatExactlyOnce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1分钟1次checkpoint
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // CheckPoint 语义 EXACTLY ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

        DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer<>(
                PvStatExactlyOnceKafkaUtil.topic, new SimpleStringSchema(), properties));

        // 按照AppId进行keyBy
        appInfoSource.keyBy((KeySelector<String, String>) appId -> appId)
                .map(new RichMapFunction<String, Tuple2<String, Long>>() {
                    private ValueState<Long> pvState;
                    private long pv = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        pvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pvStat", TypeInformation.of(new TypeHint<Long>() {
                        })));
                    }

                    @Override
                    public Tuple2<String, Long> map(String appId) throws Exception {
                        // 从状态中获取该app的pv值，+1后，update到状态中
                        if (null == pvState.value()) {
                            System.out.println(appId + " is new ,pv is 1");
                            pv = 1;
                        } else {
                            pv = pvState.value();
                            pv += 1;
                            System.out.println(appId + " is old, pv is " + pv);
                        }
                        pvState.update(pv);
                        return new Tuple2<>(appId, pv);
                    }
                })
                .print();

        env.execute("Flink pv stat");

    }

}
