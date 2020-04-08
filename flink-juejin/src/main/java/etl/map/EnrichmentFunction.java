package etl.map;

import etl.dto.OrderInfo1;
import etl.dto.OrderInfo2;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author SeawayLee
 * @create 2020-04-01 15:22
 */
public class EnrichmentFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {
    private ValueState<OrderInfo1> orderInfo1ValueState;
    private ValueState<OrderInfo2> orderInfo2ValueState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<OrderInfo1> descriptor1 = new ValueStateDescriptor<>("info1", OrderInfo1.class);
        ValueStateDescriptor<OrderInfo2> descriptor2 = new ValueStateDescriptor<>("info2", OrderInfo2.class);
        orderInfo1ValueState = getRuntimeContext().getState(descriptor1);
        orderInfo2ValueState = getRuntimeContext().getState(descriptor2);
    }

    @Override
    public void flatMap1(OrderInfo1 orderInfo1, Collector<Tuple2<OrderInfo1, OrderInfo2>> collector) throws Exception {
        // 此方法被运行，则说明第一个Stream肯定来数据了
        OrderInfo2 value2 = orderInfo2ValueState.value();
        if (value2 != null) {
            orderInfo2ValueState.clear();
            collector.collect(Tuple2.of(orderInfo1, value2));
        } else {
            orderInfo1ValueState.update(orderInfo1);
        }
    }

    @Override
    public void flatMap2(OrderInfo2 orderInfo2, Collector<Tuple2<OrderInfo1, OrderInfo2>> collector) throws Exception {
        OrderInfo1 value1 = orderInfo1ValueState.value();
        if (value1 != null) {
            orderInfo1ValueState.clear();
            collector.collect(Tuple2.of(value1, orderInfo2));
        } else {
            orderInfo2ValueState.update(orderInfo2);
        }
    }
}
