package stream.utils;

import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-04-23 11:35
 */
public class KafkaConfigUtil {
    public static Properties buildKafkaProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaUtils.BROKER_LIST);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }
}
