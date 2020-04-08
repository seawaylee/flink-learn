package state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author SeawayLee
 * @create 2020-03-30 17:01
 */
public class ReducingStateTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 3L), Tuple2.of(1L, 3L),
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0)
                .flatMap(new SumFunction())
                .print();
        env.execute("TestStatefulApi");
    }

    public static class SumFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        // 用于脑村同一个key的累加值
        private ReducingState<Long> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<>(
                    "sum",
                    (ReduceFunction<Long>) Long::sum, Long.class);
            reducingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> collector) throws Exception {
            reducingState.add(element.f1);
            collector.collect(new Tuple2<>(element.f0, reducingState.get()));
        }
    }
}
