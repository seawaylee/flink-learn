package stream.cases.pv.util;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import stream.cases.pv.dto.UserVisitWebEvent;

import java.util.Random;
import java.util.UUID;

/**
 * @author SeawayLee
 * @create 2020-05-26 15:36
 */
public class SouceDataProducer {
    public static final String yyyyMmdd = new DateTime(System.currentTimeMillis()).toString("yyyyMMdd");
    public static final Random RANDOM = new Random(System.currentTimeMillis());

    public static void mockAndSendData() throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PvKafkaConfig.getProducerProperties());
        while (true) {
            int pageId = RANDOM.nextInt(10);
            String userId = "user:" + RANDOM.nextInt(100);
            UserVisitWebEvent event = UserVisitWebEvent.builder().id(UUID.randomUUID().toString())
                    .date(yyyyMmdd)
                    .pageId(pageId)
                    .userId(userId)
                    .url("url/" + pageId)
                    .build();

            producer.send(new ProducerRecord<>(PvKafkaConfig.TOPIC, null, null, JSON.toJSONString(event)));
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        mockAndSendData();
    }
}
