package sink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import javax.xml.transform.Result;

/**
 * 累加器
 *
 * @author SeawayLee
 * @create 2020-03-30 10:50
 */
public class CounterDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            // 创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 注册累加器
                getRuntimeContext().addAccumulator("num-lines", numLines);
            }

            @Override
            public String map(String s) throws Exception {
                // 如果并行度为1，使用普通的sum++即可，但多并行度必须用累加器，否则数据可能不准确
                this.numLines.add(1);
                return s;
            }
        }).setParallelism(8);
        result.writeAsText("./acc");
        JobExecutionResult jobResult = env.execute("counter");
        // 获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num=" + num);
    }
}
