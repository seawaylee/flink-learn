package stream.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import stream.dto.ProductEvent;

import java.util.Properties;
import java.util.Random;

public class ProductUtil {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "my_product";

    public static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 10000; i++) {
            ProductEvent product = ProductEvent.builder().id((long) i)
                    .name("product" + i)
                    .price(random.nextLong() / 10000000000000L)
                    .code("code" + i).build();

            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(product));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(product));
            Thread.sleep(1000);
        }
        producer.flush();
    }
}