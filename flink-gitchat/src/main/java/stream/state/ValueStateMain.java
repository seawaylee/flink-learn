package stream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author SeawayLee
 * @create 2020-04-27 18:10
 */
public class ValueStateMain {
    /**
     * 1. ValueState
     */
    private static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                    "average",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }),
                    Tuple2.of(0L, 0L));
            sum = getRuntimeContext().getState(descriptor);

        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> collector) throws Exception {
            Tuple2<Long, Long> currentSum = sum.value();
            currentSum.f0 += 1;
            currentSum.f1 += value.f1;
            sum.update(currentSum);
            if (currentSum.f0 >= 2) {
                collector.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(2L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L));
        dataStream.keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute("State Test");


    }
}
