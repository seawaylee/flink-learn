package kafka.utils;

import com.alibaba.fastjson.JSON;
import kafka.dto.Metric;
import kafka.source.dto.Student;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-04-16 16:22
 */
public class KafkaUtils {
    public static final String BROKER_LIST = "localhost:9092";
    public static final String TOPIC = "metrics";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Metric metric = new Metric();
        metric.setName("Lee").setTsp(System.currentTimeMillis());
        Map<String, String> tags = Maps.newHashMap();
        Map<String, Object> fields = Maps.newHashMap();
        tags.put("cluster", "nikobelic");
        tags.put("host_ip", "127.0.0.1");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags).setFields(fields);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("Sending Data :" + JSON.toJSONString(metric));
        producer.flush();
    }
    public static void writeToKafkaStudent() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "zhisheng" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>("student", null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafkaStudent();
    }

}
