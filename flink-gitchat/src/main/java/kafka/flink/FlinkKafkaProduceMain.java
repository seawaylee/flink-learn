package kafka.flink;

import kafka.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

/**
 * @author SeawayLee
 * @create 2020-04-17 14:27
 */
public class FlinkKafkaProduceMain {
    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaUtils.BROKER_LIST);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
                Instant instant = Instant.ofEpochMilli(System.currentTimeMillis() + tz.getOffset(System.currentTimeMillis()));
                while (true) {
                    String outline = String.format(
                            "{\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\", \"behavior\": \"%s\", \"ts\": \"%s\"}",
                            random.nextInt(10),
                            random.nextInt(100),
                            random.nextInt(1000),
                            "pv",
                            instant.toString());
                    sourceContext.collect(outline);
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {

            }
        })
                .addSink(new FlinkKafkaProducer<>("localhost:9092", "user_behavior", new SimpleStringSchema()))
                .name("flink-connectors-kafka");
        env.execute("flink kafka connector test");
        Thread.currentThread().join();

    }
}
