package stream.broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * @author SeawayLee
 * @create 2020-06-15 10:32
 */
public class TestBroadcastSetMain {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        env.fromElements("a", "b")
                .map(new RichMapFunction<String, String>() {
                    List<Integer> broadcastData;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        broadcastData = getRuntimeContext().getBroadcastVariable("seawayLee");
                    }

                    @Override
                    public String map(String s) throws Exception {
                        return broadcastData.get(1) + s;
                    }
                }).withBroadcastSet(toBroadcast, "seawayLee").print();
        env.execute("test broadcast");
    }
}
