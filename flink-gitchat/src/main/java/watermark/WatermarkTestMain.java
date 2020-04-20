package watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author SeawayLee
 * @create 2020-04-15 10:54
 */
public class WatermarkTestMain {
    public static void produceData() {
        ThreadPoolExecutor producerPool = new ThreadPoolExecutor(5, 100, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        String topic = "user_data";
        for (int i = 0; i < 1; i++) {
            MsgProducer producer = new MsgProducer("Producer-" + i, topic);
            producerPool.execute(new Thread(producer));
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println("Starting exit")));
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);
        env.setParallelism(1);


    }
}
