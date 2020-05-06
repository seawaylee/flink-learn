package table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author SeawayLee
 * @create 2020-05-06 10:55
 */
public class TestTableApiMain {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings batchBlinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, batchBlinkSettings);

        DataStreamSource<Tuple2<Integer, String>> ds1 = env.fromElements(new Tuple2<>(1, "Hello"));
        DataStreamSource<Tuple2<Integer, String>> ds2 = env.fromElements(new Tuple2<>(1, "Hey"));

        Table table1 = tableEnv.fromDataStream(ds1, "count,word");
        Table table2 = tableEnv.fromDataStream(ds2, "count,word");
        Table resTab = table1.where("LIKE(word,'F%')").unionAll(table2);
        System.out.println(tableEnv.explain(resTab));

    }
}
