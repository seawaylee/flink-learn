package stream.flink;

import stream.utils.ESSinkUtil;
import stream.utils.KafkaUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Properties;

/**
 * @author SeawayLee
 * @create 2020-04-21 15:07
 */
public class SinkToEsMain {
    public static final String INDEX_PREFIX = "student";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaUtils.BROKER_LIST);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group-2");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>("student", new SimpleStringSchema(), props))
                .setParallelism(1);

        //dataStreamSource.print();
        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses("localhost:9200");
        int bulkSize = 10;
        int sinkParallelism = 5;

        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, dataStreamSource,
                (String student, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(INDEX_PREFIX)
                            .type(INDEX_PREFIX)
                            .source(student, XContentType.JSON));
                });
        env.execute("flink learning connectors es6");
    }
}
