package state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 值状态 测试
 * https://im/post/5e7a13915188255e245ec12a
 * 需求：当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候，就计算这些元素的 value 的平均值。计算 keyed stream 中每 3 个元素的 value 的平均值
 *
 * @author SeawayLee
 * @create 2020-03-30 14:37
 */
public class ValueStateTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 3L), Tuple2.of(1L, 3L),
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0)
                .flatMap(new CountWindowAverageWithValueState())
                .print();
        env.execute("TestStatefulApi");
    }

    /**
     * ValueState<T> ：这个状态为每一个 key 保存一个值
     * value() 获取状态值
     * update() 更新状态值
     * clear() 清除状态
     */
    public static class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        // ValueState 保存的是对应的一个 key 的一个状态值
        private ValueState<Tuple2<Long, Long>> countAndSum;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 注册状态
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>("average", Types.TUPLE(Types.LONG, Types.LONG));
            countAndSum = getRuntimeContext().getState(descriptor);
        }

        /**
         * flatMap操作会将多个输入转化为一个输出
         */
        @Override
        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {
            // 获取当前key的状态值
            Tuple2<Long, Long> currentState = countAndSum.value();
            if (currentState == null) {
                currentState = Tuple2.of(0L, 0L);
            }
            // 更新状态值中的元素个数
            currentState.f0 += 1;
            // 更新状态值中的总值
            currentState.f1 += element.f1;
            // 更新状态
            countAndSum.update(currentState);
            // 判断当前key是否出现超过3次，若是则计算平均值
            if (currentState.f0 >= 3) {
                double avg = (double) currentState.f1 / currentState.f0;
                // 输出key及其对应的平均值
                collector.collect(Tuple2.of(element.f0, avg));
                // 清空状态值
                countAndSum.clear();
            }
        }
    }

}
