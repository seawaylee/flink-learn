package stream.flink;

import stream.source.SourceFromMysql;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author SeawayLee
 * @create 2020-04-17 14:27
 */
public class CustomSourceMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMysql()).print();
        env.execute("flink kafka connector test");
    }
}
