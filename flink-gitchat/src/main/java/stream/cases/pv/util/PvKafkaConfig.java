package stream.cases.pv.util;

import stream.utils.KafkaUtils;

import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-05-26 15:40
 */
public class PvKafkaConfig {
    public static final String TOPIC = "test.page.view";

    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaUtils.BROKER_LIST);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }
    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaUtils.BROKER_LIST);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
