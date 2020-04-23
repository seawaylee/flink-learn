package stream.flink;

import stream.utils.KafkaConfigUtil;
import stream.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-04-16 16:50
 */
public class FlinkKafkaConsumeMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = KafkaConfigUtil.buildKafkaProps();

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(KafkaUtils.TOPIC, new SimpleStringSchema(), props))
                .setParallelism(1);
        // 写入另一个Topic
        dataStreamSource.print();
        env.execute("Flink add data source");
    }
}
