package stream.state;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import stream.utils.KafkaConfigUtil;

import java.util.Properties;

/**
 * Broadcast State 是 Flink 支持的另一种扩展方式，它用来支持将某一个流的数据广播到下游所有的 Task 中，
 * 数据都会存储在下游 Task 内存中，接收到广播的数据流后就可以在操作中利用这些数据，
 * 一般我们会将一些规则数据进行这样广播下去，然后其他的 Task 也都能根据这些规则数据做配置，
 * 更常见的就是规则动态的更新，然后下游还能够动态的感知。
 *
 * @author SeawayLee
 * @create 2020-04-29 17:17
 */
public class BroadcastStateMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = KafkaConfigUtil.buildKafkaProps();
        DataStreamSource<String> dataSource = env.addSource(new FlinkKafkaConsumer<>("alert", new SimpleStringSchema(), properties));

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>() {
        }));
        BroadcastStream<String> ruleBroadcastStream = dataSource.broadcast(descriptor);
        dataSource.connect(ruleBroadcastStream).process(new KeyedBroadcastProcessFunction<Object, String, String, Object>() {
            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Object> collector) throws Exception {
                
            }

            @Override
            public void processBroadcastElement(String s, Context context, Collector<Object> collector) throws Exception {

            }
        });
    }
}
