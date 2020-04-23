package stream.flink;

import com.alibaba.fastjson.JSON;
import stream.sink.SinkToMysql;
import stream.source.dto.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-04-20 16:53
 */
public class CustomSinkMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>("student", new SimpleStringSchema(), props))
                .setParallelism(1)
                .map(s -> JSON.parseObject(s, Student.class));
        student.addSink(new SinkToMysql());
        env.execute("Flink add sink");

    }
}
