package stream.cases.metric.util;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import stream.cases.metric.dto.MetricEvent;

import java.util.*;

public class BuildMachineMetricDataUtil {
    public static final String BROKER_LIST = "localhost:9092";
    public static final String METRICS_TOPIC = "lixiwei_metrics";
    public static Random random = new Random();

    public static List<String> hostIps = Arrays.asList("121.12.17.10", "121.12.17.11", "121.12.17.12", "121.12.17.13");

    public static void writeDataToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        while (true) {
            long timestamp = System.currentTimeMillis();
            for (int i = 0; i < hostIps.size(); i++) {
                MetricEvent cpuData = buildCpuData(hostIps.get(i), timestamp);
                ProducerRecord cpuRecord = new ProducerRecord<String, String>(METRICS_TOPIC, null, null, JSON.toJSONString(cpuData));
                producer.send(cpuRecord);
            }
            producer.flush();
            Thread.sleep(10000);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        writeDataToKafka();
    }

    public static MetricEvent buildCpuData(String hostIp, Long timestamp) {
        MetricEvent metricEvent = new MetricEvent();
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        int used = random.nextInt(2048);
        int max = 2048;
        metricEvent.setName("cpu");
        metricEvent.setTimestamp(timestamp);
        tags.put("cluster_name", "zhisheng");
        tags.put("host_ip", hostIp);
        fields.put("usedPercent", (double) used / max * 100);
        fields.put("used", used);
        fields.put("max", max);
        metricEvent.setFields(fields);
        metricEvent.setTags(tags);
        return metricEvent;
    }
}