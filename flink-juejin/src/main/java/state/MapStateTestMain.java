package state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * 需求同上
 *
 * @author SeawayLee
 * @create 2020-03-30 16:19
 */
public class MapStateTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 3L), Tuple2.of(1L, 3L),
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0)
                .flatMap(new CountWindowAverageWithMapState())
                .print();
        env.execute("TestStatefulApi");
    }

    public static class CountWindowAverageWithMapState
            extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        private MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("average", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> element,
                            Collector<Tuple2<Long, Double>> out) throws Exception {
            mapState.put(UUID.randomUUID().toString(), element.f1);
            ArrayList<Long> arrayList = Lists.newArrayList(mapState.values());
            if (arrayList.size() >= 3) {
                long count = 0;
                long sum = 0;
                for (Long ele : arrayList) {
                    count++;
                    sum += ele;
                }
                double avg = (double) sum / count;
                out.collect(new Tuple2<>(element.f0, avg));
                mapState.clear();
            }
        }
    }
}
