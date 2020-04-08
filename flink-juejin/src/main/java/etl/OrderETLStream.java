package etl;

import etl.dto.OrderInfo1;
import etl.dto.OrderInfo2;
import etl.map.EnrichmentFunction;
import etl.source.FileSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 两个流中订单号一样的数据合并在一起输出，不同的业务线打印出来的日志可能不太一样，
 * 所以我们其实是有挺多机会遇到这种需要把不同业务线的数据拼接起来的场景的，
 * 这就类比于一个实时的 ETL 的效果
 *
 * @author SeawayLee
 * @create 2020-03-30 18:04
 */
public class OrderETLStream {
    public static final String ORDER_INFO1_PATH = "/Users/lixiwei-mac/Documents/IdeaProjects/data-streaming/src/main/resources/order_info/OrderInfo1.txt";
    public static final String ORDER_INFO2_PATH = "/Users/lixiwei-mac/Documents/IdeaProjects/data-streaming/src/main/resources/order_info/OrderInfo2.txt";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取输入源数据流
        SingleOutputStreamOperator<OrderInfo1> orderInfo1Stream = env.addSource(new FileSource(ORDER_INFO1_PATH)).map(OrderInfo1::line2info);
        SingleOutputStreamOperator<OrderInfo2> orderInfo2Stream = env.addSource(new FileSource(ORDER_INFO2_PATH)).map(OrderInfo2::line2info);
        // 对数据按key分组
        KeyedStream<OrderInfo1, Long> keyByInfo1 = orderInfo1Stream.keyBy(OrderInfo1::getOrderId);
        KeyedStream<OrderInfo2, Long> keyByInfo2 = orderInfo2Stream.keyBy(OrderInfo2::getOrderId);
        keyByInfo1.connect(keyByInfo2).flatMap(new EnrichmentFunction()).print();

        env.execute("OrderETLStream");
    }
}
