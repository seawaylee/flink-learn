package state;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/**
 * ListState测试
 * 需求：当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候，就计算这些元素的 value 的平均值。计算 keyed stream 中每 3 个元素的 value 的平均值
 *
 * @author SeawayLee
 * @create 2020-03-30 15:42
 */
public class ListStateTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 3L), Tuple2.of(1L, 3L),
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0)
                .flatMap(new state.ValueStateTestMain.CountWindowAverageWithValueState())
                .print();
        env.execute("TestStatefulApi");
    }

    public static class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        // ListState保存当前key及其出现的次数
        private ListState<Tuple2<Long, Long>> elementByKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Tuple2<Long, Long>> descriptor = new ListStateDescriptor<>("average", Types.TUPLE(Types.LONG, Types.LONG));
            elementByKey = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {
            System.out.println("收到Element:" + element);
            Iterable<Tuple2<Long, Long>> currentState = elementByKey.get();
            // 初始化
            if (currentState == null) {
                elementByKey.addAll(Collections.emptyList());
            }
            // 更新状态
            elementByKey.add(element);
            ArrayList<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementByKey.get());
            if (allElements.size() >= 3) {
                long count = 0;
                long sum = 0;
                for (Tuple2<Long, Long> currEle : allElements) {
                    count++;
                    sum += currEle.f1;
                }
                collector.collect(Tuple2.of(element.f0, (double) sum / count));
                // 清除数据
                elementByKey.clear();
            }
        }
    }
}
