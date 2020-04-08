package broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 获取广播变量
 *
 * @author SeawayLee
 * @create 2020-03-27 17:37
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        // 运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 准备需要被广播的数据
        ArrayList<Tuple2<String, Integer>> broadSource = Lists.newArrayList();
        broadSource.add(new Tuple2<>("zhangsan", 18));
        broadSource.add(new Tuple2<>("lisi", 19));
        broadSource.add(new Tuple2<>("wangwu", 22));
        DataSource<Tuple2<String, Integer>> broadCollectionSource = env.fromCollection(broadSource);

        // 加工需要被广播的数据，将数据集转换成map类
        MapOperator<Tuple2<String, Integer>, Map<String, Integer>> toBroadcast = broadCollectionSource.map(new MapFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> map(Tuple2<String, Integer> tuple) throws Exception {
                HashMap<String, Integer> resMap = Maps.newHashMap();
                resMap.put(tuple.f0, tuple.f1);
                return resMap;
            }
        });

        // 测试元数据
        DataSource<String> testData = env.fromElements("zhangsan", "lisi", "wangwu");
        // 注意：在这里需要使用到RichMapFunction获取广播变量
        MapOperator<String, String> result = testData.map(new RichMapFunction<String, String>() {
            Map<String, Integer> allMap = new HashMap<>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             * 所以，就可以在open方法中获取广播变量数据
             */
            @Override
            public void open(Configuration parameters) {
                // 获取广播数据
                List<Map<String, Integer>> broadcastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (Map<String, Integer> broadSourceDataMap : broadcastMap) {
                    this.allMap.putAll(broadSourceDataMap);
                }
            }

            @Override
            public String map(String value) {
                Integer age = allMap.get(value);
                return value + " , " + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");
        result.print();

    }
}
