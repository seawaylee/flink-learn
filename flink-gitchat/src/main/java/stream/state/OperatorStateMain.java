package stream.state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

/**
 * 有状态的 SinkFunction 的示例，它使用 CheckpointedFunction 来缓存数据，然后再将这些数据发送到外部系统，使用了 Even-split 策略：
 *
 * @author SeawayLee
 * @create 2020-04-29 16:04
 */
public class OperatorStateMain {
    public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
        private final int threshold;
        private transient ListState<Tuple2<String, Integer>> checkpointedState;
        private List<Tuple2<String, Integer>> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = Lists.newArrayList();
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Tuple2<String, Integer> element : bufferedElements) {

                }
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Integer> element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>("buffered-elements", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
            }));
            checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
            if (functionInitializationContext.isRestored()) {
                for (Tuple2<String, Integer> element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
